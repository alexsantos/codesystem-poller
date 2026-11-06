"""
Outbox delivery admin.

    python -m src.outbox_admin status             # batch counts, oldest undelivered, failing batches
    python -m src.outbox_admin dead               # list dead (rejected) batches with their error
    python -m src.outbox_admin show <batch_id>    # print a batch's frozen Bundle
    python -m src.outbox_admin requeue <batch_id>... | --all-dead
                                                  # dead → pending, retried on the next relay cycle
    python -m src.outbox_admin discard <batch_id>...
                                                  # give up on dead batches (unblocks RELAY_ON_REJECT=block)

In Docker: docker compose exec poller python -m src.outbox_admin status
"""

from __future__ import annotations

import argparse
import json
import sys

from src.db import transaction


def _status(cur) -> None:
    cur.execute(
        """
        SELECT status, count(*) AS batches, coalesce(sum(row_count), 0) AS changes
        FROM poller.outbox_batch GROUP BY status ORDER BY status
        """
    )
    print("Batches by status:")
    for r in cur.fetchall():
        print(f"  {r['status']:<10} {r['batches']:>6} batch(es) {r['changes']:>8} change(s)")

    cur.execute(
        "SELECT count(*) AS n FROM poller.change_outbox WHERE published = false AND batch_id IS NULL"
    )
    print(f"Unsealed outbox rows: {cur.fetchone()['n']}")

    cur.execute(
        """
        SELECT id, system_url, attempts, next_attempt_at, last_error, created_at
        FROM poller.outbox_batch
        WHERE status = 'pending' AND attempts > 0
        ORDER BY seq
        """
    )
    failing = cur.fetchall()
    if failing:
        print("Pending batches with failed attempts:")
        for r in failing:
            print(f"  {r['id']} {r['system_url']} attempts={r['attempts']} "
                  f"next={r['next_attempt_at']:%Y-%m-%d %H:%M:%S%z} error={(r['last_error'] or '')[:200]}")


def _dead(cur) -> None:
    cur.execute(
        """
        SELECT id, system_url, row_count, attempts, last_attempt_at, last_error
        FROM poller.outbox_batch WHERE status = 'dead' ORDER BY seq
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("No dead batches.")
    for r in rows:
        print(f"{r['id']}  {r['system_url']}  changes={r['row_count']}  attempts={r['attempts']}  "
              f"last={r['last_attempt_at']}\n    {r['last_error']}")


def _show(cur, batch_id: str) -> None:
    cur.execute("SELECT bundle FROM poller.outbox_batch WHERE id = %s", (batch_id,))
    row = cur.fetchone()
    if row is None:
        sys.exit(f"No batch {batch_id}")
    print(json.dumps(row["bundle"], indent=2, ensure_ascii=False))


def _requeue(cur, batch_ids: list[str], all_dead: bool) -> None:
    if all_dead:
        cur.execute(
            """
            UPDATE poller.outbox_batch
            SET status = 'pending', attempts = 0, next_attempt_at = now()
            WHERE status = 'dead' RETURNING id
            """
        )
    else:
        cur.execute(
            """
            UPDATE poller.outbox_batch
            SET status = 'pending', attempts = 0, next_attempt_at = now()
            WHERE status = 'dead' AND id = ANY(%s::uuid[]) RETURNING id
            """,
            (batch_ids,),
        )
    print(f"Requeued {len(cur.fetchall())} batch(es).")


def _discard(cur, batch_ids: list[str]) -> None:
    cur.execute(
        """
        UPDATE poller.outbox_batch SET status = 'discarded'
        WHERE status = 'dead' AND id = ANY(%s::uuid[]) RETURNING id
        """,
        (batch_ids,),
    )
    print(f"Discarded {len(cur.fetchall())} batch(es). Their changes stay in change_outbox (published = false).")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m src.outbox_admin", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("dead")
    p_show = sub.add_parser("show")
    p_show.add_argument("batch_id")
    p_req = sub.add_parser("requeue")
    p_req.add_argument("batch_ids", nargs="*")
    p_req.add_argument("--all-dead", action="store_true")
    p_dis = sub.add_parser("discard")
    p_dis.add_argument("batch_ids", nargs="+")
    args = parser.parse_args(argv)

    if args.cmd == "requeue" and not (args.batch_ids or args.all_dead):
        parser.error("requeue needs batch ids or --all-dead")

    with transaction() as cur:
        if args.cmd == "status":
            _status(cur)
        elif args.cmd == "dead":
            _dead(cur)
        elif args.cmd == "show":
            _show(cur, args.batch_id)
        elif args.cmd == "requeue":
            _requeue(cur, args.batch_ids, args.all_dead)
        elif args.cmd == "discard":
            _discard(cur, args.batch_ids)


if __name__ == "__main__":
    main()
