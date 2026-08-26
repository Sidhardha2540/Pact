/**
 * Single source of truth on the client. Zustand store fed exclusively by
 * the WebSocket. The dashboard never POSTs (except for human inbox actions).
 *
 * Reducer style: each WSEvent has a handler that mutates state immutably.
 * Keep handlers small — derived UI lives in components.
 */
import { create } from 'zustand';
import type {
  Decision,
  Discovery,
  Intent,
  Participant,
  Question,
  WSEnvelope,
  FeedItem,
} from './types';

const FEED_CAP = 200; // keep the timeline bounded

interface CoordState {
  // Wire status
  connected: boolean;
  serverTime: string | null;
  setConnected: (v: boolean) => void;

  // Stores
  decisions: Decision[];
  discoveries: Discovery[];
  intents: Intent[];
  questions: Question[];
  participants: Participant[];
  feed: FeedItem[];

  // Apply a single envelope to the store.
  applyEvent: (env: WSEnvelope) => void;

  // Triage actions (HTTP)
  answerQuestion: (id: string, answer: string) => Promise<void>;
  resolveQuestion: (id: string, resolution: string) => Promise<void>;
  clearAll: () => void;
}

const COORD_HTTP =
  (typeof process !== 'undefined' && process.env.NEXT_PUBLIC_COORD_HTTP) ||
  'http://127.0.0.1:49152';

async function postHuman(path: string, body: unknown) {
  const res = await fetch(`${COORD_HTTP}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Coord-Agent-Id': 'human',
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return res.json();
}

function toFeedItem(env: WSEnvelope): FeedItem | null {
  const ts = env.ts;
  switch (env.event) {
    case 'decision_committed':
      return {
        id: `dec-${env.data.scope}-${env.data.key}-${env.data.sequence}`,
        kind: 'decision',
        agent: env.data.agent,
        scope: env.data.scope,
        summary: `${env.data.key} = ${env.data.value}`,
        ts,
      };
    case 'discovery_shared':
      return {
        id: env.data.id,
        kind: 'discovery',
        agent: env.data.agent,
        scope: env.data.scope,
        summary: env.data.summary,
        ts,
      };
    case 'intent_claimed':
      return {
        id: env.data.id,
        kind: 'intent',
        agent: env.data.agent,
        scope: env.data.scope,
        summary: `claimed: ${env.data.action}`,
        ts,
        meta: { expires_at: env.data.expires_at },
      };
    case 'intent_expired':
      return {
        id: `exp-${env.data.id}`,
        kind: 'intent',
        agent: env.data.agent,
        scope: env.data.scope,
        summary: `expired`,
        ts,
      };
    case 'intent_released':
      return {
        id: `rel-${env.data.id}`,
        kind: 'intent',
        agent: null,
        scope: env.data.scope,
        summary: `released`,
        ts,
      };
    case 'question_raised':
      return {
        id: env.data.id,
        kind: 'question',
        agent: env.data.asker_agent,
        scope: env.data.scope,
        summary: env.data.asks,
        ts,
      };
    case 'question_answered':
    case 'question_resolved':
      return {
        id: `${env.event}-${env.data.id}`,
        kind: 'question',
        agent: env.data.resolved_by,
        scope: '',
        summary:
          env.event === 'question_answered'
            ? `answered: ${env.data.answer}`
            : `resolved: ${env.data.resolution}`,
        ts,
      };
    case 'agent_registered':
      return {
        id: `reg-${env.data.agent_id}-${env.ts}`,
        kind: 'system',
        agent: env.data.agent_id,
        scope: '',
        summary: `joined: ${env.data.task}`,
        ts,
      };
    case 'coord_conflict':
      return {
        id: `cf-${env.ts}-${env.data.scope}-${env.data.code ?? 'x'}`,
        kind: 'conflict',
        agent: env.data.agent ?? null,
        scope: env.data.scope,
        summary: env.data.summary,
        ts,
        meta: env.data.code != null ? { code: env.data.code } : undefined,
      };
    default:
      return null;
  }
}

export const useCoord = create<CoordState>((set, get) => ({
  connected: false,
  serverTime: null,
  decisions: [],
  discoveries: [],
  intents: [],
  questions: [],
  participants: [],
  feed: [],

  setConnected: (connected) => set({ connected }),

  applyEvent: (env) =>
    set((s) => {
      // Always push to feed (except snapshot, which IS the feed reset).
      let nextFeed = s.feed;
      if (env.event !== 'state_snapshot') {
        const item = toFeedItem(env);
        if (item) nextFeed = [item, ...s.feed].slice(0, FEED_CAP);
      }

      switch (env.event) {
        case 'state_snapshot': {
          const reconstructedFeed: FeedItem[] = [
            ...env.data.decisions.map(d => ({
              id: `dec-${d.scope}-${d.key}-${d.sequence}`,
              kind: 'decision' as const,
              agent: d.agent,
              scope: d.scope,
              summary: `${d.key} = ${d.value}`,
              ts: d.created_at,
            })),
            ...env.data.discoveries.map(d => ({
              id: d.id,
              kind: 'discovery' as const,
              agent: d.agent,
              scope: d.scope,
              summary: d.summary,
              ts: d.created_at,
            })),
            ...env.data.intents.map(i => ({
              id: i.id,
              kind: 'intent' as const,
              agent: i.agent,
              scope: i.scope,
              summary: `claimed: ${i.action}`,
              ts: i.created_at,
              meta: { expires_at: i.expires_at }
            })),
            ...env.data.questions.map(q => ({
              id: q.id,
              kind: 'question' as const,
              agent: q.asker_agent,
              scope: q.scope,
              summary: q.asks,
              ts: q.created_at,
            }))
          ].sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime()).slice(0, FEED_CAP);

          return {
            decisions: env.data.decisions,
            discoveries: env.data.discoveries,
            intents: env.data.intents,
            questions: env.data.questions,
            participants: (env.data.participants ?? []).map((p) => ({
              ...p,
              status: p.status ?? 'online',
              last_seen: p.last_seen ?? p.registered_at,
            })),
            feed: reconstructedFeed,
            serverTime: env.data.server_time,
          };
        }

        case 'agent_registered':
          return {
            participants: [
              {
                ...env.data,
                status: env.data.status ?? 'online',
                last_seen: env.data.last_seen ?? env.data.registered_at,
              },
              ...s.participants.filter((p) => p.agent_id !== env.data.agent_id),
            ],
            feed: nextFeed,
          };

        case 'agent_status_changed':
          return {
            participants: s.participants.map((p) =>
              p.agent_id === env.data.agent_id ? { ...p, status: env.data.status } : p
            ),
            feed: nextFeed,
          };

        case 'agent_unregistered':
          return {
            participants: s.participants.filter((p) => p.agent_id !== env.data.agent_id),
            feed: nextFeed,
          };

        case 'decision_committed':
          return {
            decisions: [...s.decisions, env.data].sort((a, b) => a.sequence - b.sequence),
            feed: nextFeed,
          };

        case 'discovery_shared':
          // New discovery supersedes prior on same scope (server already
          // sends us 'discovery_superseded' events too, but be defensive).
          return {
            discoveries: [
              env.data,
              ...s.discoveries.map((d) =>
                d.scope === env.data.scope && !d.superseded
                  ? { ...d, superseded: true }
                  : d
              ),
            ],
            feed: nextFeed,
          };

        case 'discovery_superseded':
          return {
            discoveries: s.discoveries.map((d) =>
              d.id === env.data.id ? { ...d, superseded: true } : d
            ),
            feed: nextFeed,
          };

        case 'intent_claimed':
          return { intents: [env.data, ...s.intents], feed: nextFeed };

        case 'intent_refreshed':
          return {
            intents: s.intents.map((i) =>
              i.id === env.data.id ? { ...i, expires_at: env.data.expires_at } : i
            ),
            feed: nextFeed,
          };

        case 'intent_expired':
        case 'intent_released':
          return {
            intents: s.intents.filter((i) => i.id !== env.data.id),
            feed: nextFeed,
          };

        case 'question_raised':
          return { questions: [env.data, ...s.questions], feed: nextFeed };

        case 'question_answered':
          return {
            questions: s.questions.map((q) =>
              q.id === env.data.id
                ? { ...q, status: 'answered', answer: env.data.answer,
                    resolved_by: env.data.resolved_by,
                    resolved_at: env.data.resolved_at }
                : q
            ),
            feed: nextFeed,
          };

        case 'question_resolved':
          return {
            questions: s.questions.map((q) =>
              q.id === env.data.id
                ? { ...q, status: 'resolved', resolved_by: env.data.resolved_by,
                    resolved_at: env.data.resolved_at }
                : q
            ),
            feed: nextFeed,
          };

        case 'coord_conflict':
          return { feed: nextFeed };

        default:
          return { feed: nextFeed };
      }
    }),

  answerQuestion: async (id, answer) => {
    await postHuman(`/api/questions/${id}/answer`, { answer });
  },
  resolveQuestion: async (id, resolution) => {
    await postHuman(`/api/questions/${id}/resolve`, { resolution });
  },
  clearAll: () => {
    set({
      decisions: [],
      discoveries: [],
      intents: [],
      questions: [],
      participants: [],
      feed: [],
    });
  },
}));
