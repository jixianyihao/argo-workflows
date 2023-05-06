package nats

import (
	"time"

	"github.com/argoproj/argo-workflows/v3/config"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

type NatsSender interface {
	Send(workflowId string, workflow []byte)
}

type natsSender struct {
	Conn *nats.Conn

	JetStream nats.JetStream

	StreamConfig nats.StreamConfig
}

func NewNatsSender(config config.NatsConfig) NatsSender {

	// Create an unauthenticated connection to NATS.
	nc, _ := nats.Connect(config.ServerUrl)

	// Drain is a safe way to to ensure all buffered messages that were published
	// are sent and all buffered messages received on a subscription are processed
	// being closing the connection.
	defer nc.Drain()

	js, _ := nc.JetStream()
	cfg := nats.StreamConfig{
		Name:     config.SubjectName,
		Subjects: []string{config.Subject},
	}

	// JetStream provides both file and in-memory storage options. For
	// durability of the stream data, file storage must be chosen to
	// survive crashes and restarts. This is the default for the stream,
	// but we can still set it explicitly.
	cfg.Storage = nats.FileStorage

	// Finally, let's add/create the stream with the default (no) limits.
	js.AddStream(&cfg)

	return &natsSender{nc, js, cfg}
}

func (natsSender *natsSender) Send(workflowId string, workflow []byte) {
	start := time.Now()
	_, err := natsSender.JetStream.Publish(natsSender.StreamConfig.Name, workflow)
	if err != nil {  
	        log.WithError(err).Error("workflow pub failed err")
	}
	since := time.Since(start)

	if since > 1*time.Second {
		log.Warn("pub workflow  %s cost too long", workflowId)
	}
}
