// Package coretls is how a module trusts core's TLS certificate when it
// reaches core over HTTPS: the channels' sync call, control link and
// connection sockets, a factory's factory link. By default, as any HTTPS
// client, it trusts the system's certificate authorities. Three settings,
// the same in every module, trust a core whose certificate they do not
// sign (a self-signed one):
//
//	LOGOS_CORE_CA_FILE       a PEM file of certificates to trust beside the
//	                         system's: core's own self-signed certificate, or
//	                         the authority that issued it. Still verified,
//	                         host name included.
//	LOGOS_CORE_CERT_SHA256   the SHA-256 fingerprint of core's certificate
//	                         (hex, colons allowed, as openssl and core's
//	                         nginx image print it): exactly that
//	                         certificate is accepted, whatever signed it and
//	                         whatever name it carries.
//	LOGOS_CORE_TLS_INSECURE  true: no verification at all. Anyone between
//	                         the module and core can read and change what
//	                         passes. Logged as a warning.
//
// One of them at a time: the first that is set, in the order insecure,
// fingerprint, CA file, decides. Client is the client those settings make,
// built once for the process; channel-core and factory-core use it for every
// request to core a module makes without a client of its own.
package coretls

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

// The settings' environment variables.
const (
	EnvCAFile     = "LOGOS_CORE_CA_FILE"
	EnvCertSHA256 = "LOGOS_CORE_CERT_SHA256"
	EnvInsecure   = "LOGOS_CORE_TLS_INSECURE"
)

// Config is how a module trusts core's certificate.
type Config struct {
	// CAFile is a PEM file of certificates trusted beside the system's.
	CAFile string
	// CertSHA256 is the fingerprint of the one certificate accepted: the
	// SHA-256 of its DER, 32 bytes.
	CertSHA256 []byte
	// Insecure skips verification.
	Insecure bool
}

// FromEnv reads the settings from the environment (lookup, os.LookupEnv
// when nil). An error says which setting does not read.
func FromEnv(lookup func(string) (string, bool)) (Config, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	var c Config
	if v, ok := lookup(EnvInsecure); ok && strings.TrimSpace(v) != "" {
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return c, fmt.Errorf("%s: %q is not true or false", EnvInsecure, v)
		}
		c.Insecure = b
	}
	if v, ok := lookup(EnvCertSHA256); ok && strings.TrimSpace(v) != "" {
		fp, err := ParseFingerprint(v)
		if err != nil {
			return c, fmt.Errorf("%s: %w", EnvCertSHA256, err)
		}
		c.CertSHA256 = fp
	}
	if v, ok := lookup(EnvCAFile); ok {
		c.CAFile = strings.TrimSpace(v)
	}
	return c, nil
}

// ParseFingerprint reads a SHA-256 fingerprint: 64 hex digits, colons or
// spaces between them allowed, either case ("AB:CD:…", as openssl prints it).
func ParseFingerprint(s string) ([]byte, error) {
	h := strings.NewReplacer(":", "", " ", "", "-", "").Replace(strings.TrimSpace(s))
	h = strings.TrimPrefix(strings.TrimPrefix(h, "sha256/"), "SHA256=")
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != sha256.Size {
		return nil, errors.New("a SHA-256 fingerprint is 64 hex digits")
	}
	return b, nil
}

// Fingerprint is a certificate's SHA-256 fingerprint as openssl prints it:
// upper-case hex pairs joined by colons.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

// Mode names what decides, for logs: "system", "ca-file", "fingerprint" or
// "insecure".
func (c Config) Mode() string {
	switch {
	case c.Insecure:
		return "insecure"
	case c.CertSHA256 != nil:
		return "fingerprint"
	case c.CAFile != "":
		return "ca-file"
	}
	return "system"
}

// TLS is the TLS configuration c makes; nil for the system's defaults.
func (c Config) TLS() (*tls.Config, error) {
	switch c.Mode() {
	case "insecure":
		return &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, nil //nolint:gosec // asked for
	case "fingerprint":
		want := c.CertSHA256
		return &tls.Config{
			MinVersion: tls.VersionTLS12,
			// The chain and the name are not what is checked: the leaf is,
			// byte for byte, in VerifyPeerCertificate.
			InsecureSkipVerify: true, //nolint:gosec // verified below
			VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
				if len(raw) == 0 {
					return errors.New("core sent no certificate")
				}
				got := sha256.Sum256(raw[0])
				if !bytes.Equal(got[:], want) {
					return fmt.Errorf("core's certificate is %s, not the one %s names", Fingerprint(raw[0]), EnvCertSHA256)
				}
				return nil
			},
		}, nil
	case "ca-file":
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", EnvCAFile, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: %s holds no PEM certificate", EnvCAFile, c.CAFile)
		}
		return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
	}
	return nil, nil
}

// HTTPClient is an HTTP client trusting core's certificate as c says: the
// default transport's settings, with c's TLS.
func (c Config) HTTPClient() (*http.Client, error) {
	tc, err := c.TLS()
	if err != nil {
		return nil, err
	}
	if tc == nil {
		return &http.Client{}, nil
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = tc
	return &http.Client{Transport: t}, nil
}

var (
	once      sync.Once
	defClient *http.Client
	defErr    error
)

// Client is the process's client for core, from the environment's
// settings, built once (and logged once: insecure as a warning). Its error
// says which setting does not read; every request to core fails with it
// until the setting is fixed.
func Client() (*http.Client, error) {
	once.Do(func() {
		c, err := FromEnv(nil)
		if err == nil {
			defClient, err = c.HTTPClient()
		}
		defErr = err
		switch {
		case err != nil:
			slog.Error("core TLS settings", "err", err)
		case c.Insecure:
			slog.Warn("core TLS: certificate not verified (" + EnvInsecure + "): anyone between this module and core can read and change what passes")
		case c.Mode() != "system":
			slog.Info("core TLS", "trust", c.Mode())
		}
	})
	return defClient, defErr
}
