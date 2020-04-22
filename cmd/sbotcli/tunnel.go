package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/shurcooL/go-goon"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/muxrpc"
	"go.cryptoscope.co/ssb"
	clientPkg "go.cryptoscope.co/ssb/client"
	"go.cryptoscope.co/ssb/message"
	"go.cryptoscope.co/ssb/plugins/whoami"
	"gopkg.in/urfave/cli.v2"
)

var tunnelCmd = &cli.Command{
	Name:  "tunnel",
	Usage: "just testing some stuff",
	Action: func(ctx *cli.Context) error {
		resp, err := client.Async(longctx, map[string]interface{}{}, muxrpc.Method{"tunnel", "isRoom"})
		if err != nil {
			return errors.Wrapf(err, "isRoom")
		}
		log.Log("event", "isRoom!")
		goon.Dump(resp)

		edpv, err := client.Async(longctx, []ssb.FeedRef{}, muxrpc.Method{"tunnel", "endpoints"})
		if err != nil {
			return errors.Wrapf(err, "listing endpoints failed")
		}
		goon.Dump(edpv)

		endpoints, ok := edpv.([]ssb.FeedRef)
		if !ok {
			return fmt.Errorf("wrong type: %T", edpv)
		}

		if len(endpoints) < 1 {
			return fmt.Errorf("no endpoints")
		}

		type qry struct {
			Target ssb.FeedRef `json:"target"`
		}
		src, snk, err := client.Duplex(longctx, []byte{}, muxrpc.Method{"tunnel", "connect"}, qry{Target: endpoints[0]})
		if err != nil {
			return errors.Wrapf(err, "tunnel.connect failed")
		}

		r := muxrpc.NewSourceReader(src)
		w := muxrpc.NewSinkWriter(snk)

		pseudeRw := &pseudoConn{Reader: r, WriteCloser: w}

		var pubKey = make(ed25519.PublicKey, ed25519.PublicKeySize)
		copy(pubKey[:], endpoints[0].ID)

		wrapper := client.Handshake.ConnWrapper(pubKey)
		conn, err := wrapper(pseudeRw)
		if err != nil {
			return errors.Wrap(err, "error dialing  over tunnel")
		}

		pkr := muxrpc.NewPacker(conn)

		h := whoami.New(log, &ssb.FeedRef{ID: []byte("hahahahaha!")}).Handler()

		edp := muxrpc.HandleWithRemote(pkr, h, conn.RemoteAddr())

		srv, ok := edp.(muxrpc.Server)
		if !ok {
			conn.Close()
			return errors.Errorf("ssbClient: failed to cast handler to muxrpc server (has type: %T)", edp)
		}
		tunCtx, cancel := context.WithCancel(longctx)
		defer cancel()
		go func() {
			err = srv.Serve(tunCtx)
			if err != nil {
				level.Warn(log).Log("event", "tunnel muxrpc.Serve exited", "err", err)
			}
			conn.Close()
		}()

		tunClient, err := clientPkg.FromEndpoint(edp, clientPkg.WithContext(tunCtx))
		if err != nil {
			return errors.Wrap(err, "error dialing over tunnel")
		}

		args := message.CreateHistArgs{}
		args.Limit = 20
		args.Reverse = true
		args.ID, err = ssb.ParseFeedRef("@LtQ3tOuLoeQFi5s/ic7U6wDBxWS3t2yxauc4/AwqfWc=.ed25519")
		if err != nil {
			panic(err)
		}

		msgSrc, err := tunClient.CreateHistoryStream(args)
		if err != nil {
			return errors.Wrap(err, "historystream call failed")
		}
		err = luigi.Pump(longctx, jsonDrain(os.Stdout), msgSrc)

		time.Sleep(30 * time.Second)
		conn.Close()

		return nil
	},
}

type pseudoConn struct {
	io.Reader
	io.WriteCloser

	local, remote net.Addr
}

var _ net.Conn = (*pseudoConn)(nil)

// LocalAddr returns the local net.Addr with the local public key
func (conn *pseudoConn) LocalAddr() net.Addr {
	return conn.local
}

// RemoteAddr returns the remote net.Addr with the remote public key
func (conn *pseudoConn) RemoteAddr() net.Addr {
	return conn.remote
}

// SetDeadline passes the call to the underlying net.Conn
func (conn *pseudoConn) SetDeadline(t time.Time) error {
	panic("TODO: implement setDeadline")
	// return conn.conn.SetDeadline(t)
}

// SetReadDeadline passes the call to the underlying net.Conn
func (conn *pseudoConn) SetReadDeadline(t time.Time) error {
	panic("TODO: implement setReadDeadline")
}

// SetWriteDeadline passes the call to the underlying net.Conn
func (conn *pseudoConn) SetWriteDeadline(t time.Time) error {
	panic("TODO: implement setWriteDeadline")
}
