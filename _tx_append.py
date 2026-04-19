# ── Cash Account ───────────────────────────────────────────────────────────────

def db_get_cash_account(user_id: int) -> Optional[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cash_accounts (user_id, balance)
                VALUES (%s, 0)
                ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id
                RETURNING id, user_id, balance, updated_at
                """,
                (user_id,),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


# ── Transactions ───────────────────────────────────────────────────────────────

class TransactionError(Exception):
    pass


def db_create_transaction(
    user_id: int,
    tx_type: str,
    amount: float,
    secid: Optional[str] = None,
    quantity: Optional[float] = None,
    price: Optional[float] = None,
    commission: float = 0,
    tx_date: Optional[str] = None,
) -> dict:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cash_accounts (user_id, balance) VALUES (%s, 0) "
                "ON CONFLICT (user_id) DO NOTHING",
                (user_id,),
            )

            if tx_type == "buy":
                total_cost = amount + commission
                cur.execute(
                    "SELECT balance FROM cash_accounts WHERE user_id = %s FOR UPDATE",
                    (user_id,),
                )
                row = cur.fetchone()
                if not row or float(row[0]) < total_cost:
                    raise TransactionError(
                        f"Недостаточно средств: нужно {total_cost:.2f}, доступно {row[0] if row else 0:.2f}"
                    )
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance - %s, updated_at = now() WHERE user_id = %s",
                    (total_cost, user_id),
                )
                cur.execute(
                    """
                    INSERT INTO portfolio_positions (portfolio_id, secid, quantity, avg_price)
                    SELECT p.id, %s, %s, %s
                    FROM portfolios p WHERE p.user_id = %s LIMIT 1
                    ON CONFLICT (portfolio_id, secid) DO UPDATE SET
                        quantity  = portfolio_positions.quantity  + EXCLUDED.quantity,
                        avg_price = (
                            portfolio_positions.quantity  * portfolio_positions.avg_price
                            + EXCLUDED.quantity         * EXCLUDED.avg_price
                        ) / (portfolio_positions.quantity + EXCLUDED.quantity)
                    """,
                    (secid, quantity, price, user_id),
                )

            elif tx_type == "sell":
                cur.execute(
                    """
                    SELECT pp.quantity, p.id
                    FROM portfolio_positions pp
                    JOIN portfolios p ON p.id = pp.portfolio_id
                    WHERE p.user_id = %s AND pp.secid = %s
                    FOR UPDATE OF pp
                    """,
                    (user_id, secid),
                )
                row = cur.fetchone()
                if not row or float(row[0]) < float(quantity or 0):
                    raise TransactionError(
                        f"Недостаточно бумаг: нужно {quantity}, в наличии {row[0] if row else 0}"
                    )
                net_proceeds = amount - commission
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance + %s, updated_at = now() WHERE user_id = %s",
                    (net_proceeds, user_id),
                )
                cur.execute(
                    "UPDATE portfolio_positions SET quantity = quantity - %s WHERE portfolio_id = %s AND secid = %s",
                    (quantity, row[1], secid),
                )
                cur.execute(
                    "DELETE FROM portfolio_positions WHERE portfolio_id = %s AND secid = %s AND quantity <= 0",
                    (row[1], secid),
                )

            elif tx_type in ("coupon", "deposit"):
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance + %s, updated_at = now() WHERE user_id = %s",
                    (amount, user_id),
                )

            elif tx_type == "withdraw":
                cur.execute(
                    "SELECT balance FROM cash_accounts WHERE user_id = %s FOR UPDATE",
                    (user_id,),
                )
                row = cur.fetchone()
                if not row or float(row[0]) < amount:
                    raise TransactionError(
                        f"Недостаточно средств для вывода: нужно {amount:.2f}, доступно {row[0] if row else 0:.2f}"
                    )
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance - %s, updated_at = now() WHERE user_id = %s",
                    (amount, user_id),
                )

            cur.execute(
                """
                INSERT INTO transactions (user_id, secid, type, quantity, price, amount, commission, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s::timestamp, now()))
                RETURNING id, user_id, secid, type, quantity, price, amount, commission, date, created_at
                """,
                (user_id, secid, tx_type, quantity, price, amount, commission, tx_date),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


def db_list_transactions(
    user_id: int,
    limit: int = 100,
    offset: int = 0,
    tx_type: Optional[str] = None,
    secid: Optional[str] = None,
) -> List[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            conditions = ["user_id = %s"]
            params: List = [user_id]
            if tx_type:
                conditions.append("type = %s")
                params.append(tx_type)
            if secid:
                conditions.append("secid = %s")
                params.append(secid)
            where = " AND ".join(conditions)
            cur.execute(
                f"""
                SELECT id, user_id, secid, type, quantity, price, amount, commission, date, created_at
                FROM transactions
                WHERE {where}
                ORDER BY date DESC, created_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def db_sync_positions_from_transactions(user_id: int) -> None:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM portfolios WHERE user_id = %s LIMIT 1", (user_id,))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO portfolios (user_id, name) VALUES (%s, 'Main') RETURNING id",
                    (user_id,),
                )
                portfolio_id = cur.fetchone()[0]
            else:
                portfolio_id = row[0]
            cur.execute("DELETE FROM portfolio_positions WHERE portfolio_id = %s", (portfolio_id,))
            cur.execute(
                """
                INSERT INTO portfolio_positions (portfolio_id, secid, quantity, avg_price)
                SELECT %s, secid, SUM(quantity), SUM(quantity * price) / NULLIF(SUM(quantity), 0)
                FROM transactions
                WHERE user_id = %s AND type = 'buy'
                GROUP BY secid
                HAVING SUM(quantity) > 0
                """,
                (portfolio_id, user_id),
            )
            conn.commit()