package monoceros

import (
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/c12s/plumtree"
)

const SCORE_MSG_TYPE data.MessageType = plumtree.GRAFT_MSG_TYPE + 1

type ScoreMsg struct {
	Scores map[string]int
}

func Score() int {
	return 1
}

func (m *Monoceros) HasHigherScore(nodeID string, score int) bool {
	return score > Score() || (score == Score() && nodeID > m.config.NodeID)
}

func startPeriodic(fn func(), interval time.Duration) {
	go func() {
		for range time.NewTicker(interval).C {
			fn()
		}
	}()
}

func (n *TreeOverlay) broadcastScore() {
	payload := ScoreMsg{
		Scores: map[string]int{n.plumtree.Protocol.Self().ID: Score()},
	}
	n.lock.Lock()
	for _, p := range n.plumtree.GetPeers() {
		scores, ok := n.knownScores[p.Node.ID]
		if !ok {
			continue
		}
		payload.Scores[p.Node.ID] = scores[p.Node.ID]
	}
	n.lock.Unlock()
	msg := data.Message{
		Type:    SCORE_MSG_TYPE,
		Payload: payload,
	}
	for _, p := range n.plumtree.GetPeers() {
		err := p.Conn.Send(msg)
		if err != nil {
			n.logger.Println(err)
		}
	}
}

func (n *TreeOverlay) onScoreMsg(msgBytes []byte, sender hyparview.Peer) {
	msg := &ScoreMsg{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		n.logger.Println("failed to deserialize Score message", "error", err)
		return
	}
	n.updateScores(sender.Node.ID, msg.Scores)
}

func (n *TreeOverlay) updateScores(from string, scores map[string]int) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.knownScores[from] = scores
	n.logger.Println("SCORES AFTER UPDATE", n.knownScores)
}

func (n *TreeOverlay) removeScoreForPeer(nodeID string) {
	n.lock.Lock()
	defer n.lock.Unlock()
	delete(n.knownScores, nodeID)
	for _, scores := range n.knownScores {
		delete(scores, nodeID)
	}
	n.logger.Println("SCORES AFTER REMOVE", n.knownScores)
}

// locked by caller
func (n *TreeOverlay) highestScoreInNeighborhood() bool {
	selfID := n.plumtree.Protocol.Self().ID
	maxID := selfID
	maxScore := Score()
	for _, scores := range n.knownScores {
		for id, score := range scores {
			if score > maxScore || (score == maxScore && id > maxID) {
				maxID = id
				maxScore = score
			}
		}
	}
	return maxID == selfID
}
