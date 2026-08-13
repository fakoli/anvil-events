# The origin story

*How one small question — "what's actually running over there?" — became a
third wheel for a family of projects.*

---

## 1. The setup

I run several machines, and every one of them earns its keep differently.

One box is the heavy lifter: it serves the primary models — the big,
GPU-hungry weights that take both cards just to think. Another, smaller
machine handles the lighter, side-of-desk work: voice, experiments, image
workflows. And then there are the laptops — the machines that run the agents
— the gateways I actually talk to every day.

Here's the thing: these machines are not independent. They look like separate
computers, but they're really **one system spread across boxes**. Each one
contributes a different capability, and they all rely on knowing about each
other — not just to route traffic between them, but to coordinate: multiple
AI models, multiple agents, all of them needing to agree on what the current
configuration actually is.

## 2. The first itch

Updating the model choice on one system is easy. The hard part is the question
that follows it:

*"Did I copy the settings from one place to another?"*

Because the configuration doesn't live in one place. It lives half in a repo,
half in an operator home directory, half in Docker volumes on the machines
themselves, and — on the bad days — half in my head. Changing something on one
box and hoping the rest of the fleet finds out is exactly the kind of thing
you can't verify by looking at any single machine.

## 3. The experiment that disappeared

One afternoon I was experimenting on the primary model host. A session
running directly on that machine promoted a newly arrived model: the serving
process came up, health checks passed, the router quietly started sending
traffic to the new weights. Everything was working.

But nothing told the rest of the fleet.

The repo still described the old world. The other boxes still believed the old
truth. Even the gateway I was talking to — the agent on another machine — had
no idea. The moment I asked "what's actually running over there right now?"
the answer had to be discovered the hard way: probing live endpoints,
inspecting containers, diffing configs. Twenty minutes of archaeology to
establish what a single event should have said instantly, the moment it
happened.

And the kicker: a service on the smaller machine — a completely legit part of
the fleet — existed in reality and in *no record at all*. Reality and the
records disagreed, and reality won, but there was no automatic mechanism to
tell anyone the records were wrong.

## 4. The conversation that broke

The frustrating version of this story goes like this: I'm talking to an agent
on one machine, and it can't tell me what's running on another machine — not
because it's stupid, but because **the information had never traveled**. The
change was made by a session working directly on the target host; the
lifecycle transaction happened entirely in that machine's world; nothing
published, nothing journaled, nothing notified.

This isn't a routing problem. It's not even a *config* problem in the usual
sense. It's a gap in the loop between "something changed" and "everyone who
needs to know, knows."

## 5. The reframe

My first instinct was the standard distributed-systems grab bag: shared
consensus store, cluster filesystem, a ZooKeeper-of-my-own. But thinking it
through, the fleet doesn't have that disease. There's one writer at a time —
whoever is running the promotion. There's no leader election to do, no quorum
to chase.

The real shape of the problem is simpler and more honest:

> **The promotion is the event.**

If every lifecycle change *publishes* — announces itself to a bus every host
and every agent gateway subscribes to — then the moment something changes,
everyone learns. The repo stays the declared spec, but the *event* becomes the
authoritative record of what happened, and the gateways update themselves. No
box-hopping. No "did I copy the settings?" No twenty-minute archaeology.

## 6. The name

So this is the third wheel of a small family:

- **anvil** coordinates *who* does what — the orchestration layer.
- **anvil-serving** serves *what* models on which tiers — the serving layer.
- **anvil-events** records *what happened* — and tells everyone who needs
  to know.

Small, typed, versioned lifecycle events. An append-only journal. A bus that's
just a whisper away on every machine. Not a platform — a contract.

## 7. The lab bench

The best part of personal software is that it's a place to test ideas you'd
never get to try at work. Designing the delivery contract meant standing on
published shoulders:

- the log as the journal — the oldest idea in distributed systems,
- an exactly-once replay protocol for the outbox,
- causal consistency as a *checkable* property, not a vibe,
- and a 2026 paper arguing that logical clocks are epistemic orderings, not
  global truth — which justified keeping per-producer ordering and refusing
  the temptation of a global sequencer.

Every design decision in this repo has a paper it can point at. That's not
decoration; that's the point of building it.

## 8. The promise

The test of success is boring, which is exactly how you know it worked:

> Start work on any machine. Change a model, promote a tier, alter the
> serving config. Never wonder whether the settings made it anywhere — because
> the change announced itself, the journal recorded it, and the gateways
> already know.

One system, spread across boxes, finally acting like one system.
