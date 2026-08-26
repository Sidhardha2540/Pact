/**
 * Mirror of backend/models.py. Keep in sync.
 *
 * Event names mirror backend/state_engine.py and ws_broadcast.publish() calls.
 * If you add a new event server-side, add it here and handle it in
 * lib/store.ts.
 */

export type AgentId = 'claude-code' | 'cursor' | 'aider' | 'human' | string;

export type ConflictCode = 409 | 410 | 423 | 429 | 404 | 403;

export interface Decision {
  scope: string;
  key: string;
  value: string;
  agent: AgentId;
  rationale: string | null;
  created_at: string;
  sequence: number;
}

export interface Discovery {
  id: string;
  scope: string;
  summary: string;
  file_hash: string | null;
  agent: AgentId;
  confidence: 'unverified' | 'verified' | 'contradicted';
  created_at: string;
  superseded: boolean;
  stale?: boolean;
}

export interface Intent {
  id: string;
  scope: string;
  action: string;
  agent: AgentId;
  created_at: string;
  expires_at: string;
  status: 'active' | 'expired' | 'released' | 'completed';
}

export interface Question {
  id: string;
  scope: string;
  asks: string;
  asker_agent: AgentId;
  target: string;
  blocking: boolean;
  status: 'open' | 'answered' | 'resolved' | 'deferred';
  answer: string | null;
  resolved_by: string | null;
  created_at: string;
  resolved_at: string | null;
}

export type ParticipantStatus = 'online' | 'idle' | 'offline';

export interface Participant {
  agent_id: string;
  type: 'agent' | 'human';
  task: string;
  scope: string[];
  role_tag?: string | null;
  mode: 'exclusive' | 'collaborative';
  registered_at: string;
  status: ParticipantStatus;
  last_seen?: string;
}

export interface StateSnapshot {
  decisions: Decision[];
  discoveries: Discovery[];
  intents: Intent[];
  questions: Question[];
  participants?: Participant[];
  server_time: string;
}

// ---------------------------------------------------------------------
// WebSocket events
// ---------------------------------------------------------------------

export type WSEvent =
  | { event: 'state_snapshot';      data: StateSnapshot }
  | { event: 'agent_registered';   data: Participant }
  | { event: 'agent_unregistered'; data: { agent_id: string } }
  | { event: 'agent_status_changed'; data: { agent_id: string; status: ParticipantStatus } }
  | { event: 'decision_committed';  data: Decision }
  | { event: 'discovery_shared';    data: Discovery }
  | { event: 'discovery_superseded';data: { id: string; scope: string } }
  | { event: 'intent_claimed';      data: Intent }
  | { event: 'intent_refreshed';    data: { id: string; scope: string; agent: AgentId; expires_at: string } }
  | { event: 'intent_expired';      data: { id: string; scope: string; agent: AgentId } }
  | { event: 'intent_released';     data: { id: string; scope: string } }
  | { event: 'question_raised';     data: Question }
  | { event: 'question_answered';   data: { id: string; answer: string; resolved_by: string; resolved_at: string } }
  | { event: 'question_resolved';   data: { id: string; resolution: string; resolved_by: string; resolved_at: string } }
  | {
      event: 'coord_conflict';
      data: {
        summary: string;
        scope: string;
        code?: number;
        agent?: AgentId | null;
      };
    };

export type WSEnvelope = WSEvent & { ts: string };

// ---------------------------------------------------------------------
// Activity feed
// ---------------------------------------------------------------------

/** Unified row for the center timeline. The store derives these from events. */
export interface FeedItem {
  id: string;
  kind: 'decision' | 'discovery' | 'intent' | 'question' | 'system' | 'conflict';
  agent: AgentId | null;
  scope: string;
  summary: string;
  ts: string;
  meta?: Record<string, unknown>;
}
