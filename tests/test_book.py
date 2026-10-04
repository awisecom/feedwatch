from __future__ import annotations

from feedwatch.book import Result, SymbolBook
from feedwatch.model import Level, Snapshot, Update


def conf(seq: int, qty: float = 1.0, price: float = 100.0) -> Update:
    return Update("ALFA", seq, Level.CONFIRMED, price, qty, 1000.0 + seq)


def prov(seq: int, qty: float = 1.0, price: float = 100.0) -> Update:
    return Update("ALFA", seq, Level.PROVISIONAL, price, qty, 1000.0 + seq)


def synced(seq: int = 0, cum_qty: float = 0.0) -> SymbolBook:
    book = SymbolBook("ALFA")
    book.apply_snapshot(Snapshot("ALFA", seq, 100.0, cum_qty, 1000.0))
    return book


def test_confirmed_updates_apply_in_order() -> None:
    book = synced()
    for seq in (1, 2, 3):
        assert book.apply(conf(seq, qty=seq)) is Result.APPLIED
    assert (book.seq, book.cum_qty) == (3, 6.0)


def test_nothing_is_applied_before_a_baseline() -> None:
    book = SymbolBook("ALFA")
    assert book.apply(conf(5)) is Result.BUFFERED
    assert book.needs_baseline()
    book.apply_snapshot(Snapshot("ALFA", 4, 100.0, 10.0, 1004.0))
    assert (book.seq, book.cum_qty) == (5, 11.0)


def test_a_snapshot_newer_than_the_buffer_wins() -> None:
    book = SymbolBook("ALFA")
    book.apply(conf(5))
    book.apply(conf(6))
    book.apply_snapshot(Snapshot("ALFA", 6, 100.0, 20.0, 1006.0))
    assert (book.seq, book.cum_qty) == (6, 20.0)  # not 22: 5 and 6 are inside the snapshot
    assert not book.buffer


def test_duplicates_are_counted_and_ignored() -> None:
    book = synced()
    book.apply(conf(1))
    assert book.apply(conf(1)) is Result.DUPLICATE
    assert book.cum_qty == 1.0
    assert book.stats.duplicates == 1


def test_a_gap_buffers_until_the_backfill_arrives() -> None:
    book = synced()
    book.apply(conf(1))
    assert book.apply(conf(4)) is Result.GAP
    assert book.apply(conf(5)) is Result.BUFFERED
    assert book.missing() == (2, 3)
    assert book.seq == 1  # nothing past the hole is applied yet

    assert book.apply_backfill([conf(3), conf(2)]) == 2
    assert (book.seq, book.cum_qty) == (5, 5.0)
    assert (book.stats.gaps, book.stats.backfilled) == (1, 2)
    assert book.missing() is None


def test_a_partial_backfill_leaves_the_rest_missing() -> None:
    book = synced()
    book.apply(conf(1))
    book.apply(conf(4))
    book.apply_backfill([conf(2)])
    assert book.seq == 2
    assert book.missing() == (3, 3)


def test_a_snapshot_closes_a_gap_the_upstream_no_longer_has() -> None:
    book = synced()
    book.apply(conf(1))
    book.apply(conf(10))
    book.apply(conf(11))
    book.apply_snapshot(Snapshot("ALFA", 10, 100.0, 50.0, 1010.0))
    assert (book.seq, book.cum_qty) == (11, 51.0)


def test_an_older_snapshot_is_ignored() -> None:
    book = synced(seq=5, cum_qty=9.0)
    assert book.apply_snapshot(Snapshot("ALFA", 3, 99.0, 7.0, 1003.0)) is False
    assert (book.seq, book.cum_qty) == (5, 9.0)


def test_a_heartbeat_exposes_a_loss_at_the_tail() -> None:
    book = synced()
    book.apply(conf(1))
    assert book.note_upstream_seq(3) is True  # 2 and 3 exist upstream, nothing came after them
    assert book.missing() == (2, 3)
    book.apply_backfill([conf(2), conf(3)])
    assert book.missing() is None
    assert book.note_upstream_seq(3) is False


def test_provisional_data_never_touches_confirmed_state() -> None:
    book = synced()
    assert book.apply(prov(1, price=101.0)) is Result.PROVISIONAL
    assert (book.seq, book.price) == (0, 100.0)
    assert book.prov_price == 101.0


def test_a_confirmation_that_differs_is_a_revision() -> None:
    book = synced()
    book.apply(prov(1, price=101.0, qty=2.0))
    book.apply(conf(1, price=100.5, qty=2.0))
    assert book.stats.revisions == 1
    (rev,) = book.take_revisions()
    assert (rev.provisional_price, rev.confirmed_price) == (101.0, 100.5)


def test_a_matching_confirmation_is_not_a_revision() -> None:
    book = synced()
    book.apply(prov(1))
    book.apply(conf(1))
    assert book.stats.revisions == 0


def test_committed_updates_are_handed_out_once() -> None:
    book = synced()
    book.apply(conf(1))
    book.apply(conf(2))
    assert [u.seq for u in book.take_committed()] == [1, 2]
    assert book.take_committed() == []


def test_trusting_provisional_data_drifts_while_confirmed_state_does_not() -> None:
    """The original bug in miniature: one revised event and the running total is wrong."""
    events = [(1, 2.0, 2.0), (2, 3.0, 1.5), (3, 1.0, 1.0)]  # (seq, provisional qty, confirmed qty)
    book = synced()
    trusted_provisional = 0.0
    for seq, p_qty, c_qty in events:
        book.apply(prov(seq, qty=p_qty))
        trusted_provisional += p_qty
        book.apply(conf(seq, qty=c_qty))
    assert trusted_provisional == 6.0
    assert book.cum_qty == 4.5  # what actually happened
    assert book.stats.revisions == 1
