// SPDX-License-Identifier: MIT

package gossip

import (
	"context"
	"fmt"
	"sync"

	"github.com/cryptix/go/logging"
	"github.com/go-kit/kit/metrics"
	"go.cryptoscope.co/luigi"
	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/margaret/multilog"
	"go.cryptoscope.co/muxrpc"
	"go.cryptoscope.co/ssb"
)

type HMACSecret *[32]byte

type Promisc bool

func New(
	ctx context.Context,
	log logging.Interface,
	id *ssb.FeedRef,
	rootLog margaret.Log,
	userFeeds multilog.MultiLog,
	wantList ssb.ReplicationLister,
	opts ...interface{},
) *plugin {
	h := &handler{
		self:       id,
		receiveLog: rootLog,
		feedIndex:  userFeeds,
		wantList:   wantList,
		logger:     log,
		rootCtx:    ctx,
	}

	for i, o := range opts {
		switch v := o.(type) {
		case metrics.Gauge:
			h.sysGauge = v
		case metrics.Counter:
			h.sysCtr = v
		case HMACSecret:
			h.hmacSec = v
		case Promisc:
			h.promisc = bool(v)
		default:
			log.Log("warning", "unhandled option", "i", i, "type", fmt.Sprintf("%T", o))
		}
	}

	h.pushManager = NewFeedPushManager(
		h.rootCtx,
		h.receiveLog,
		h.feedIndex,
		h.logger,
		h.sysGauge,
		h.sysCtr,
	)

	h.pull = &pullManager{
		self:      *id,
		wantList:  wantList,
		feedIndex: userFeeds,

		receiveLog: rootLog,
		append: &rxSink{
			logger: log,
			append: rootLog,
		},

		verifyMu:    &sync.Mutex{},
		verifySinks: make(map[string]luigi.Sink),

		hmacKey: h.hmacSec,

		logger: log,
	}

	return &plugin{h}
}

func NewHist(
	ctx context.Context,
	log logging.Interface,
	id *ssb.FeedRef,
	rootLog margaret.Log,
	userFeeds multilog.MultiLog,
	wantList ssb.ReplicationLister,
	opts ...interface{},
) histPlugin {
	h := &handler{
		self:       id,
		receiveLog: rootLog,
		feedIndex:  userFeeds,
		wantList:   wantList,

		logger:  log,
		rootCtx: ctx,
	}

	for i, o := range opts {
		switch v := o.(type) {
		case metrics.Gauge:
			h.sysGauge = v
		case metrics.Counter:
			h.sysCtr = v
		case Promisc:
			h.promisc = bool(v)
		case HMACSecret:
			h.hmacSec = v
		default:
			log.Log("warning", "unhandled hist option", "i", i, "type", fmt.Sprintf("%T", o))
		}
	}

	h.pushManager = NewFeedPushManager(
		h.rootCtx,
		h.receiveLog,
		h.feedIndex,
		h.logger,
		h.sysGauge,
		h.sysCtr,
	)

	return histPlugin{h}
}

type plugin struct{ h *handler }

func (plugin) Name() string              { return "gossip" }
func (plugin) Method() muxrpc.Method     { return muxrpc.Method{"gossip"} }
func (p plugin) Handler() muxrpc.Handler { return p.h }

type histPlugin struct{ h *handler }

func (hp histPlugin) Name() string       { return "createHistoryStream" }
func (histPlugin) Method() muxrpc.Method { return muxrpc.Method{"createHistoryStream"} }

func (*histPlugin) HandleConnect(ctx context.Context, edp muxrpc.Endpoint) {}
func (h *histPlugin) HandleCall(ctx context.Context, req *muxrpc.Request, edp muxrpc.Endpoint) {
	h.h.HandleCall(ctx, req, edp)
}

func (hp *histPlugin) Handler() muxrpc.Handler { return hp }
