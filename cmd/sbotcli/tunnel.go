package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"go.cryptoscope.co/muxrpc/debug"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/shurcooL/go-goon"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/muxrpc"
	"go.cryptoscope.co/ssb"
	clientPkg "go.cryptoscope.co/ssb/client"
	"go.cryptoscope.co/ssb/message"
	"gopkg.in/urfave/cli.v2"
)

func tryEndpointsAsync(me *ssb.FeedRef, avail chan<- ssb.FeedRef) error {
	edpv, err := client.Async(longctx, []ssb.FeedRef{}, muxrpc.Method{"tunnel", "endpoints"})
	if err != nil {
		return errors.Wrapf(err, "listing endpoints failed")

	}

	endpoints, ok := edpv.([]ssb.FeedRef)
	if !ok {
		err = fmt.Errorf("wrong type: %T", edpv)
		return err
	}

	go func() {
		for _, edp := range endpoints {
			if !edp.Equal(me) {
				log.Log("new", edp.Ref())
				avail <- edp
			}
		}
		close(avail)
	}()

	return nil
}

func tryEndpointsSource(me *ssb.FeedRef, avail chan<- ssb.FeedRef) error {
	var v []*ssb.FeedRef
	edpStream, err := client.Source(longctx, v, muxrpc.Method{"tunnel", "endpoints"})
	if err != nil {
		err = errors.Wrapf(err, "listing endpoints failed")
		level.Warn(log).Log("err", err)
		return nil
	}

	for {
		refv, err := edpStream.Next(longctx)
		if err != nil {
			if luigi.IsEOS(err) {
				break
			}
			return err
		}
		frs, ok := refv.([]*ssb.FeedRef)
		if !ok {
			spew.Dump(refv)
			level.Warn(log).Log("msg", "wrong type from source", "T", fmt.Sprintf("%T", refv))
			return nil
		}
		for _, edp := range frs {
			if !edp.Equal(me) {
				log.Log("new", edp.Ref())
				avail <- *edp
			}
		}
	}

	return nil
}

var tunnelCmd = &cli.Command{
	Name:  "tunnel",
	Usage: "just testing some stuff",
	Action: func(ctx *cli.Context) error {
		me := client.Self()
		if me == nil {
			return errors.Errorf("whoami failed")
		}
		resp, err := client.Async(longctx, map[string]interface{}{}, muxrpc.Method{"tunnel", "isRoom"})
		if err != nil {
			return errors.Wrapf(err, "isRoom")
		}
		log.Log("event", "isRoom!")
		goon.Dump(resp)

		var v interface{}
		resp, err = client.Async(longctx, v, muxrpc.Method{"tunnel", "ping"})
		if err != nil {
			return errors.Wrapf(err, "ping")
		}
		goon.Dump(resp)

		ch := make(chan ssb.FeedRef)
		var available <-chan ssb.FeedRef = ch

		// available := tryEndpointsAsync(me,ch)

		// if available == nil {
		go func() {
			err := tryEndpointsSource(me, ch)
			level.Error(log).Log("no-more-endpoints", err)
			time.Sleep(1 * time.Minute)
			close(ch)
		}()

		for {

			select {
			case newPeer, ok := <-available:
				if !ok {
					log.Log("msg", "exiting")
					return nil
				}
				go func() {
					err := tryTunnel(newPeer)
					level.Info(log).Log("peer-done", err)
				}()
			case <-time.After(30 * time.Second):
				level.Info(log).Log("still", "waiting")
			}
		}

		return nil
	},
}

func tryTunnel(remote ssb.FeedRef) error {
	level.Info(log).Log("event", "connecting to", "ref", remote.Ref())
	var qry = struct {
		Target ssb.FeedRef `json:"target"`
		Portal string      `json:"portal"`
	}{
		Target: remote,
		Portal: string(""),
	}

	src, snk, err := client.Duplex(longctx, []byte{}, muxrpc.Method{"tunnel", "connect"}, qry)
	if err != nil {
		return errors.Wrapf(err, "tunnel.connect failed")
	}

	r := muxrpc.NewSourceReader(src)
	w := muxrpc.NewSinkWriter(snk)

	pseudeRw := &pseudoConn{Reader: r, WriteCloser: w}

	var pubKey = make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(pubKey[:], remote.ID)

	wrapper := client.Handshake.ConnWrapper(pubKey)
	conn, err := wrapper(pseudeRw)
	if err != nil {
		return errors.Wrap(err, "error dialing  over tunnel")
	}

	pkr := muxrpc.NewPacker(debug.WrapConn(log, conn))

	h := noopHandler{}

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

	return conn.Close()
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

type noopHandler struct{}

func (h noopHandler) HandleConnect(ctx context.Context, edp muxrpc.Endpoint) {
}

func (h noopHandler) HandleCall(ctx context.Context, req *muxrpc.Request, edp muxrpc.Endpoint) {
	level.Warn(log).Log("new-call", req.Method.String())
	// req.Stream.CloseWithError(fmt.Errorf("go-ssb/client: unsupported call"))
}
