/**
 * raft.js (Production-grade Raft)
 *
 * - Proper randomized election timeouts
 * - Leader election with vote collection
 * - Heartbeat management
 * - Handles lost and restored peers
 * - Uses your stable logger
 */

const axios = require('axios');
const logger = require('../logWrapper/logger');

class RaftNode {
    constructor(nodeId, peers = [], dnId, rpUrl,nodeUrl) {
        this.nodeId = nodeId;
        this.peers = peers;
        this.dnId = dnId;
        this.rpUrl = rpUrl;
        this.nodeUrl = nodeUrl;

        this.currentTerm = 0;
        this.votedFor = null;
        this.state = 'follower';
        this.leaderId = null;

        this.votesReceived = 0;
        this.unreachablePeers = new Set();

        logger.trace(`[Raft] Initializing RaftNode for ${this.nodeId}`, 30001);
        this.startElectionTimer();
    }

    startElectionTimer() {
        const timeout = 1000 + Math.random() * 500;
        clearTimeout(this.electionTimeout);
        this.electionTimeout = setTimeout(() => this.startElection(), timeout);
    }

    async startElection() {
        this.state = 'candidate';
        this.currentTerm++;
        this.votedFor = this.nodeId;
        this.votesReceived = 1;
        this.leaderId = null;
        logger.trace(`[Raft] ${this.nodeId} starting election for term ${this.currentTerm}`, 30002);

        const voteRequest = {
            term: this.currentTerm,
            candidateId: this.nodeId
        };

        for (const peer of this.peers) {
            try {
                const res = await axios.post(`${peer}/raft/vote`, voteRequest, {
                    timeout: 1500,
                    headers: { Connection: 'close' }
                });
                if (res.data.voteGranted) {
                    this.votesReceived++;
                    logger.info(`[Raft] Vote from ${peer}`, 30003);
                } else {
                    logger.trace(`[Raft] Vote rejected from ${peer}`, 30004);
                }
            } catch (err) {
                logger.warn(`[Raft] No response from ${peer}: ${err.message}`, 30005);
            }
        }

        const majority = Math.floor(this.peers.length / 2) + 1;
        if (this.votesReceived >= majority) {
            this.becomeLeader();
        } else {
            logger.info(`[Raft] Not enough votes, reverting to follower`, 30006);
            this.state = 'follower';
            this.startElectionTimer();
        }
    }

    becomeLeader() {
        this.state = 'leader';
        this.leaderId = this.nodeId;
        logger.trace(`[Raft] ${this.nodeId} became leader for term ${this.currentTerm}`, 30011);
        this.notifyRP();

        this.sendHeartbeats();
        this.heartbeatInterval = setInterval(() => this.sendHeartbeats(), 150);
    }

    async sendHeartbeats() {
        for (const peer of this.peers) {
            try {
                const res = await axios.post(
                    `${peer}/raft/heartbeat`,
                    { term: this.currentTerm, leaderId: this.nodeId },
                    { timeout: 1500, headers: { Connection: 'close' } }
                );
                if (res.status === 200) {
                    if (this.unreachablePeers.has(peer)) {
                        this.unreachablePeers.delete(peer);
                        logger.info(`[Raft] Connection restored with ${peer}`, 30012);
                    }
                    logger.trace(`[Raft] Heartbeat sent to ${peer}`, 30013);
                } else {
                    logger.warn(`[Raft] Heartbeat response ${res.status} from ${peer}`, 30014);
                }
            } catch (err) {
                if (!this.unreachablePeers.has(peer)) {
                    this.unreachablePeers.add(peer);
                    logger.warn(`[Raft] Lost connection to ${peer}: ${err.message}`, 30015);
                }
            }
        }
    }

    async notifyRP() {
        if (!this.rpUrl) {
            logger.warn('[Raft] RP URL not configured, cannot notify master election.', 30021);
            return;
        }
        try {
            const notifyUrl = `${this.rpUrl}/set_master?dnId=${this.dnId}&masterId=${this.nodeId}&masterUrl=${this.nodeUrl}&term=${this.currentTerm}`;
            logger.trace(`[Raft] Notifying RP of master: GET ${notifyUrl}`, 30022);
            const res = await axios.get(notifyUrl);
            logger.trace(`[Raft] RP response: ${JSON.stringify(res.data)}`, 30023);
        } catch (err) {
            logger.error(`[Raft] Failed to notify RP: ${err.message}. Retrying in 3 seconds...`, 30024);
            setTimeout(() => this.notifyRP(), 3000);
        }
    }

    handleVoteRequest(req, res) {
        const { term, candidateId } = req.body;
        let voteGranted = false;

        if (term >= this.currentTerm && this.votedFor === null) {
            this.votedFor = candidateId;
            this.currentTerm = term;
            this.state = 'follower';
            this.leaderId = null;
            this.startElectionTimer();
            voteGranted = true;
            logger.info(`[Raft] 🤝 Voted for ${candidateId} (term ${term})`, 30025);
        }

        res.json({ voteGranted });
    }

    handleHeartbeat(req, res) {
        const { term, leaderId } = req.body;
        if (term >= this.currentTerm) {
            this.currentTerm = term;
            this.votedFor = null;
            this.state = 'follower';
            this.leaderId = leaderId || null;
            this.startElectionTimer();
            logger.trace(`[Raft] Heartbeat from ${leaderId} (term ${term})`, 30026);
        } else {
            logger.trace(`[Raft] Rejected heartbeat from ${leaderId} due to lower term ${term}`, 30027);
        }
        res.status(200).json({ status: 'ok' });
    }

    getRole() {
        return this.state;
    }

    getTerm() {
        return this.currentTerm;
    }

    getLeader() {
        return this.leaderId;
    }
}

module.exports = RaftNode;
