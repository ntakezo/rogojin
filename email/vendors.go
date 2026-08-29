package email

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/emersion/go-message/mail"
)

// endpoint fixes the IMAP address each vendor is reached at.
func endpoint(v Vendor) (string, error) {
	switch v {
	case Gmail:
		return "imap.gmail.com:993", nil
	case Outlook:
		return "outlook.office365.com:993", nil
	}
	return "", fmt.Errorf("%w: %q", ErrVendorUnknown, v)
}

// tokenURL fixes where each vendor mints access tokens from refresh tokens,
// unless the Auth names its own (a tenant-specific Microsoft endpoint, say).
func tokenURL(v Vendor, a Auth) string {
	if a.TokenURL != "" {
		return a.TokenURL
	}
	switch v {
	case Outlook:
		return "https://login.microsoftonline.com/common/oauth2/v2.0/token"
	default:
		return "https://oauth2.googleapis.com/token"
	}
}

// validateInbox rejects at Add time what would otherwise fail at the first
// dial, far from the write that caused it.
func validateInbox(in Inbox) error {
	if _, err := endpoint(in.Vendor); err != nil {
		return err
	}
	switch in.Auth.Kind {
	case AuthPassword:
		if in.Vendor != Gmail {
			return fmt.Errorf("vendor %s retired password auth; use %s", in.Vendor, AuthOAuth2)
		}
		if in.Auth.Password == "" {
			return errors.New("password auth requires a password")
		}
	case AuthOAuth2:
		if in.Auth.ClientID == "" || in.Auth.RefreshToken == "" {
			return errors.New("oauth2 auth requires a client id and a refresh token")
		}
	default:
		return fmt.Errorf("unknown auth kind %q", in.Auth.Kind)
	}
	return nil
}

// dialIMAP opens and authenticates the vendor session for e; it is the
// production dialer behind every listener and backfill.
func dialIMAP(e Email) (Mailbox, error) {
	if e.Inbox == nil {
		return nil, ErrNoInbox
	}
	in := *e.Inbox
	addr, err := endpoint(in.Vendor)
	if err != nil {
		return nil, err
	}
	username := in.Auth.Username
	if username == "" {
		username = e.Address
	}

	// wake is signaled by unilateral mailbox updates during IDLE; buffered
	// so an update landing outside Idle is kept for the next call rather
	// than lost.
	wake := make(chan struct{}, 1)
	client, err := imapclient.DialTLS(addr, &imapclient.Options{
		UnilateralDataHandler: &imapclient.UnilateralDataHandler{
			Mailbox: func(*imapclient.UnilateralDataMailbox) {
				select {
				case wake <- struct{}{}:
				default:
				}
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	switch in.Auth.Kind {
	case AuthPassword:
		err = client.Login(username, in.Auth.Password).Wait()
	case AuthOAuth2:
		var token string
		if token, err = fetchAccessToken(in.Vendor, in.Auth); err == nil {
			err = client.Authenticate(xoauth2{username: username, token: token})
		}
	default:
		err = fmt.Errorf("unknown auth kind %q", in.Auth.Kind)
	}
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("authenticate %s: %w", username, err)
	}
	return &imapMailbox{client: client, wake: wake}, nil
}

// fetchAccessToken mints a short-lived access token from the stored refresh
// token. Dials are rare — one per listener session — so nothing is cached.
func fetchAccessToken(v Vendor, a Auth) (string, error) {
	form := url.Values{
		"client_id":     {a.ClientID},
		"refresh_token": {a.RefreshToken},
		"grant_type":    {"refresh_token"},
	}
	if a.ClientSecret != "" {
		form.Set("client_secret", a.ClientSecret)
	}
	resp, err := http.PostForm(tokenURL(v, a), form)
	if err != nil {
		return "", fmt.Errorf("refresh token: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("refresh token: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("refresh token: %s: %s", resp.Status, body)
	}
	var minted struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &minted); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if minted.AccessToken == "" {
		return "", errors.New("token response carried no access_token")
	}
	return minted.AccessToken, nil
}

// xoauth2 is the SASL XOAUTH2 mechanism both vendors accept: one initial
// response, no challenge round-trips a client can answer.
type xoauth2 struct {
	username string
	token    string
}

func (x xoauth2) Start() (string, []byte, error) {
	return "XOAUTH2", []byte("user=" + x.username + "\x01auth=Bearer " + x.token + "\x01\x01"), nil
}

func (x xoauth2) Next(challenge []byte) ([]byte, error) {
	// The only challenge XOAUTH2 sends is a JSON error blob.
	return nil, fmt.Errorf("xoauth2 rejected: %s", challenge)
}

// imapMailbox adapts one authenticated go-imap session to the mailbox seam.
type imapMailbox struct {
	client *imapclient.Client
	wake   chan struct{}
}

var fetchOptions = &imap.FetchOptions{
	UID:         true,
	Envelope:    true,
	BodySection: []*imap.FetchItemBodySection{{}},
}

func (mb *imapMailbox) Select(ctx context.Context) (uint32, uint32, error) {
	data, err := mb.client.Select("INBOX", nil).Wait()
	if err != nil {
		return 0, 0, fmt.Errorf("select INBOX: %w", err)
	}
	var lastUID uint32
	if data.UIDNext > 0 {
		lastUID = uint32(data.UIDNext) - 1
	}
	return data.UIDValidity, lastUID, nil
}

func (mb *imapMailbox) FetchSince(ctx context.Context, uid uint32) ([]Message, error) {
	set := imap.UIDSet{imap.UIDRange{Start: imap.UID(uid + 1), Stop: 0}}
	bufs, err := mb.client.Fetch(set, fetchOptions).Collect()
	if err != nil {
		return nil, fmt.Errorf("uid fetch: %w", err)
	}
	msgs := make([]Message, 0, len(bufs))
	for _, buf := range bufs {
		// UID FETCH n:* returns the mailbox's last message even when
		// nothing is newer; the cursor comparison filters it out.
		if uint32(buf.UID) <= uid {
			continue
		}
		msgs = append(msgs, toMessage(buf))
	}
	return msgs, nil
}

func (mb *imapMailbox) FetchSinceDate(ctx context.Context, since time.Time) ([]Message, error) {
	data, err := mb.client.UIDSearch(&imap.SearchCriteria{Since: since}, nil).Wait()
	if err != nil {
		return nil, fmt.Errorf("uid search: %w", err)
	}
	uids := data.AllUIDs()
	if len(uids) == 0 {
		return nil, nil
	}
	bufs, err := mb.client.Fetch(imap.UIDSetNum(uids...), fetchOptions).Collect()
	if err != nil {
		return nil, fmt.Errorf("uid fetch: %w", err)
	}
	msgs := make([]Message, 0, len(bufs))
	for _, buf := range bufs {
		msgs = append(msgs, toMessage(buf))
	}
	return msgs, nil
}

// Idle parks on the server until new mail may exist, ctx ends, or the
// session breaks — a session that ends IDLE on its own is a broken one.
func (mb *imapMailbox) Idle(ctx context.Context) error {
	cmd, err := mb.client.Idle()
	if err != nil {
		return fmt.Errorf("idle: %w", err)
	}
	ended := make(chan error, 1)
	go func() { ended <- cmd.Wait() }()

	select {
	case <-ctx.Done():
		cmd.Close()
		<-ended
		return ctx.Err()
	case <-mb.wake:
		cmd.Close()
		return <-ended
	case err := <-ended:
		if err == nil {
			err = errors.New("session ended IDLE unilaterally")
		}
		return fmt.Errorf("idle: %w", err)
	}
}

func (mb *imapMailbox) Close() error {
	return mb.client.Close()
}

// toMessage flattens one fetched message: addressing from the envelope,
// text and HTML bodies from the MIME parts. A body that will not parse
// still delivers its envelope — the workflow owns interpretation.
func toMessage(buf *imapclient.FetchMessageBuffer) Message {
	msg := Message{UID: uint32(buf.UID)}
	if env := buf.Envelope; env != nil {
		msg.MessageID = env.MessageID
		msg.Subject = env.Subject
		msg.Date = env.Date
		if len(env.From) > 0 {
			msg.From = strings.ToLower(env.From[0].Addr())
		}
		for _, to := range env.To {
			msg.To = append(msg.To, strings.ToLower(to.Addr()))
		}
	}
	for _, section := range buf.BodySection {
		text, html := readBodies(section.Bytes)
		if msg.Text == "" {
			msg.Text = text
		}
		if msg.HTML == "" {
			msg.HTML = html
		}
	}
	return msg
}

// readBodies walks the message's inline MIME parts collecting the first
// text/plain and text/html bodies.
func readBodies(raw []byte) (text, html string) {
	r, err := mail.CreateReader(strings.NewReader(string(raw)))
	if err != nil {
		return "", ""
	}
	for {
		part, err := r.NextPart()
		if err != nil {
			return text, html
		}
		header, ok := part.Header.(*mail.InlineHeader)
		if !ok {
			continue
		}
		mediaType, _, err := header.ContentType()
		if err != nil {
			continue
		}
		body, err := io.ReadAll(part.Body)
		if err != nil {
			continue
		}
		switch {
		case mediaType == "text/plain" && text == "":
			text = string(body)
		case mediaType == "text/html" && html == "":
			html = string(body)
		}
	}
}
