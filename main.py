import os
import re
import math
import logging
import requests
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, List, Tuple
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'bonds_db'),
    'user':     os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '15021502'),
    'options':  '-c client_encoding=UTF8',
}

DAYS_IN_YEAR = 365

_BOND_SUFFIX_RE = re.compile(
    r'\s*[-–]?\s*(?:'
    r'БО[-\s]|ПБО[-\s]|'
    r'об(?:л)?\.?\s*(?:сер)?|'
    r'сер(?:ия)?\.?\s*|'
    r'выпуск\s*|вып\.?\s*|'
    r'\d+-[йя]\s+(?:вып|сер)|'
    r'(?:[\w-]+\s*)?\d{2,}[-\s](?:летн|год)|'
    r'\b\d{1,2}[-\s](?:лет|год)'
    r')',
    re.IGNORECASE | re.UNICODE,
)


# ======================================================================
# Фин расчёты
# ======================================================================

def calc_reliability_score(
    list_level: Optional[int],
    volume: Optional[float],
    duration: Optional[float],
    issue_size: Optional[float],
    face_value: Optional[float],
    is_qualified: bool,
    close_price: Optional[float],
    coupon_type: Optional[str],
    has_offer: bool,
) -> Optional[float]:
    """
    Расчёт скай reliability_score от 0 до 100.
    Чем выше — тем надёжнее облигация.

    Критерии:
    - list_level: 1 = лучший, 2-3 = средний, отсутствует = плохо
    - volume: высокий объём = надёжнее
    - duration: короткая дюрация = надёжнее (меньше риска)
    - issue_size: большой выпуск = надёжнее
    - is_qualified: бумаги для квалинвесторов обычно рискованнее
    - close_price: без цены = подозрительно
    - coupon_type: FLOAT/ZERO = выше риск
    - has_offer: наличие оферты = доп. неопределённость
    """
    if close_price is None:
        return None  # невозможно оценить без цены

    score = 0.0
    max_score = 100.0

    # 1. Уровень листинга (до 30 баллов)
    if list_level == 1:
        score += 30
    elif list_level == 2:
        score += 20
    elif list_level == 3:
        score += 10
    # отсутствует = 0

    # 2. Объём торгов (до 20 баллов)
    if volume is not None:
        if volume >= 10_000_000:  # > 10 млн
            score += 20
        elif volume >= 1_000_000:  # > 1 млн
            score += 15
        elif volume >= 100_000:   # > 100 тыс
            score += 10
        elif volume >= 10_000:    # > 10 тыс
            score += 5
        # < 10 тыс = 0

    # 3. Дюрация (до 15 баллов) — короткая = надёжнее
    if duration is not None:
        if duration <= 365:
            score += 15
        elif duration <= 730:  # до 2 лет
            score += 10
        elif duration <= 1825:  # до 5 лет
            score += 5
        # > 5 лет = 0

    # 4. Размер выпуска (до 15 баллов)
    if issue_size and face_value:
        total_issued = issue_size
        if total_issued >= 10_000_000_000:  # > 10 млрд
            score += 15
        elif total_issued >= 1_000_000_000:  # > 1 млрд
            score += 10
        elif total_issued >= 100_000_000:   # > 100 млн
            score += 5
        # < 100 млн = 0

    # 5. Для квалинвесторов (минус 15 баллов)
    if is_qualified:
        score -= 15

    # 6. Тип купона (минус 10 баллов для FLOAT/ZERO)
    if coupon_type in ('FLOAT', 'VARIABLE'):
        score -= 10
    elif coupon_type == 'ZERO':
        score -= 5

    # 7. Наличие оферты (минус 5 баллов)
    if has_offer:
        score -= 5

    # Итог: нормализуем в диапазон 0-100
    return max(0.0, min(100.0, score))


def is_junk_bond(
    list_level: Optional[int],
    volume: Optional[float],
    reliability_score: Optional[float],
    close_price: Optional[float],
    is_qualified: bool,
    maturity: Optional[date],
    days_to_maturity: Optional[int] = None,
) -> Tuple[bool, str]:
    """
    Определяет, явл��ется ли облигация "мусорной".
    Возвращает (is_junk: bool, reason: str)

    Критерии мусора:
    - Нет листинга (list_level = None)
    - Сверхнизкая надёжность (< 20)
    - Нет цены (закрыта/не торгуется)
    - Объём торгов < 10 000 руб/день
    - Для квалинвесторов с низкой ликвидностью
    - Срок погашения < 30 дней (技术 default)
    - Очень длинная дюрация > 10 лет (> 3650 дней) при низкой надёжности
    """
    if close_price is None:
        return True, "Нет цены (не торгуется)"

    # Нет листинга - потенциально мусор
    if list_level is None:
        if volume is None or (volume is not None and volume < 50_000):
            return True, "Нет листинга и низкий объём"

    # Критерии по надёжности
    if reliability_score is not None:
        if reliability_score < 20:
            return True, f"Низкая надёжность ({reliability_score:.0f})"

    # Сверхнизкая ликвидность
    if volume is not None and volume < 10_000:
        return True, f"Сверхнизкая ликвидность (объём < 10 000)"

    # Для квалинвесторов с низким объёмом
    if is_qualified and (volume is None or volume < 100_000):
        return True, "Бумага для квалинвесторов с низкой ликвидностью"

    # Срок погашения
    if maturity is not None:
        today = date.today()
        if days_to_maturity is None:
            days_to_maturity = (maturity - today).days

        if days_to_maturity < 30:
            return True, f"Срок погашения < 30 дней ({days_to_maturity})"
        if days_to_maturity > 3650 and (reliability_score is None or reliability_score < 40):
            return True, f"Долгосрочная бумага (> 10 лет) с низкой надёжностью"

    return False, ""


def parse_issuer(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    m = _BOND_SUFFIX_RE.search(name)
    if m:
        issuer = name[:m.start()].strip().rstrip(',-–( ')
        if len(issuer) >= 3:
            return issuer
    parts = name.rsplit(None, 1)
    if len(parts) == 2 and re.search(r'\d', parts[1]):
        candidate = parts[0].strip().rstrip(',-–( ')
        if len(candidate) >= 3:
            return candidate
    return name.strip() or None


def _days_between(d1: date, d2: date) -> int:
    return (d2 - d1).days


def calc_nkd(
    settlement: date,
    maturity: date,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
    coupon_type: str,
    next_coupon_str: Optional[str] = None,
) -> float:
    """Накопленный купонный доход."""
    if coupon_type in ('FLOAT', 'VARIABLE', 'ZERO'):
        return 0.0
    if not all([coupon_value, coupon_period, face_value]):
        return 0.0

    coupon_days = coupon_period if coupon_period > 0 else 182

    # Если знаем дату следующего купона — считаем точно
    if next_coupon_str:
        try:
            next_coupon_date = datetime.strptime(
                str(next_coupon_str)[:10], '%Y-%m-%d'
            ).date()
            prev_coupon_date = next_coupon_date - timedelta(days=coupon_days)
            if prev_coupon_date <= settlement < next_coupon_date:
                days_accrued = (settlement - prev_coupon_date).days
                nkd = (days_accrued / coupon_days) * coupon_value
                return round(max(0.0, min(nkd, coupon_value)), 4)
        except (ValueError, TypeError):
            pass

    if not maturity:
        return 0.0

    # Ищем ближайшую купонную дату назад от погашения
    t = maturity
    while t >= settlement:
        diff = (t - settlement).days
        if diff >= 0 and diff % coupon_days == 0:
            prev_coupon = t - timedelta(days=coupon_days) if diff > 0 else t
            days_accrued = max(0, (settlement - prev_coupon).days)
            nkd = (days_accrued / coupon_days) * coupon_value
            return round(max(0.0, min(nkd, coupon_value)), 4)
        t -= timedelta(days=1)

    return 0.0


def _pv_total(
    annual_rate: float,
    n_periods: float,
    coupon_value: float,
    face_value: float,
    periods_per_year: float,
) -> float:
    """Текущая стоимость облигации при заданной годовой ставке."""
    if periods_per_year <= 0:
        return float('inf')
    r = annual_rate / periods_per_year  # ставка за период
    if r <= -0.9999:
        return float('inf')
    if abs(r) < 1e-10:
        return coupon_value * n_periods + face_value
    return (
        coupon_value * (1 - (1 + r) ** (-n_periods)) / r
        + face_value / (1 + r) ** n_periods
    )


def _calc_ytm_hybrid(
    settlement: date,
    end_date: date,
    dirty_price: float,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
) -> Optional[float]:
    """
    YTM гибридным методом: бинарный поиск (1–2 итерации) для нахождения
    отрезка, затем метод Ньютона внутри отрезка.
    end_date — либо maturity_date (YTM), либо offer_date (YTC).
    ytm возвращается как годовая ставка (0.15 = 15% годовых).
    """
    if not dirty_price or dirty_price <= 0:
        return None
    if not face_value or face_value == 0:
        return None
    if not end_date or end_date <= settlement:
        return None
    if not coupon_period or coupon_period <= 0:
        return None

    coupon_days = coupon_period
    periods_per_year = DAYS_IN_YEAR / coupon_days
    days_total = _days_between(settlement, end_date)
    n_periods = periods_per_year * days_total / DAYS_IN_YEAR

    if n_periods <= 0:
        return None

    # --- Шаг 1: бинарный поиск (2 итерации) — нашли отрезок [lo_annual, hi_annual] ---
    # отрезок поиска: годовая ставка от 0.01% до 200%
    lo_annual = 0.0001
    hi_annual = 2.0

    for _ in range(50):  # достаточно для большинства случаев
        mid = (lo_annual + hi_annual) / 2.0
        pv_mid = _pv_total(mid, n_periods, coupon_value, face_value, periods_per_year)
        if pv_mid > dirty_price:
            lo_annual = mid
        else:
            hi_annual = mid
        if hi_annual - lo_annual < 1e-8:
            break

    # --- Шаг 2: Ньютон внутри отрезка ---
    ytm = (lo_annual + hi_annual) / 2.0
    ytm = max(lo_annual, min(ytm, hi_annual))

    for _ in range(100):
        r = ytm / periods_per_year  # ставка за период
        if r <= -0.9999:
            break

        price_at_ytm = _pv_total(ytm, n_periods, coupon_value, face_value, periods_per_year)

        # Производная d(PV)/d(ytm) через конечную разность
        dr = max(ytm * 1e-8, 1e-12)
        dp_dr = (
            _pv_total(ytm + dr, n_periods, coupon_value, face_value, periods_per_year)
            - _pv_total(max(0.0, ytm - dr), n_periods, coupon_value, face_value, periods_per_year)
        ) / (2 * dr)

        if abs(dp_dr) < 1e-15:
            break

        delta = (price_at_ytm - dirty_price) / dp_dr
        ytm -= delta

        if abs(delta) < 1e-10:
            break

        # Ограничиваем внутри отрезка, найденного бинарным поиском
        ytm = max(lo_annual, min(ytm, hi_annual))

    if ytm < 0 or ytm > 2 or math.isnan(ytm) or math.isinf(ytm):
        return None
    return ytm


def calc_ytm_newton(
    settlement: date,
    maturity: date,
    dirty_price: float,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
    coupon_type: str,
) -> Optional[float]:
    """YTM гибридным методом (бинарный поиск + Ньютон)."""
    if coupon_type in ('FLOAT', 'VARIABLE'):
        return None
    return _calc_ytm_hybrid(
        settlement, maturity, dirty_price,
        face_value, coupon_value, coupon_period,
    )


def calc_ytc(
    settlement: date,
    offer_date: date,
    dirty_price: float,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
    coupon_type: str,
) -> Optional[float]:
    """Yield to Call (YTC) — доходность к оферте."""
    if coupon_type in ('FLOAT', 'VARIABLE'):
        return None
    return _calc_ytm_hybrid(
        settlement, offer_date, dirty_price,
        face_value, coupon_value, coupon_period,
    )


def calc_ytm(
    settlement: date,
    maturity: date,
    clean_price: float,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
    coupon_type: str,
    next_coupon_str: Optional[str] = None,
) -> Tuple[Optional[float], float]:
    """
    Возвращает (ytm, nkd).
    ytm — доходность к погашению (0.15 = 15% годовых).
    nkd — накопленный купонный доход в рублях.
    """
    if maturity <= settlement:
        return None, 0.0
    if not clean_price or clean_price <= 0:
        return None, 0.0

    # Дисконтные облигации — вычисляем аналитически (coupon_period может быть 0)
    if coupon_type == 'ZERO':
        years = _days_between(settlement, maturity) / DAYS_IN_YEAR
        if years > 0 and clean_price > 0:
            ytm = (face_value / clean_price) ** (1 / years) - 1
            return max(0.0, ytm), 0.0
        return None, 0.0

    if not all([settlement, maturity, face_value, coupon_period]):
        return None, 0.0

    # Флоатеры — YTM не считаем
    if coupon_type in ('FLOAT', 'VARIABLE'):
        return None, 0.0

    nkd = calc_nkd(
        settlement, maturity, face_value,
        coupon_value, coupon_period, coupon_type, next_coupon_str
    )
    # dirty_price в рублях: (clean_price — в % от номинала) * номинал + НКД
    dirty = (clean_price / 100.0) * face_value + nkd
    ytm = calc_ytm_newton(
        settlement, maturity, dirty,
        face_value, coupon_value, coupon_period, coupon_type
    )
    return ytm, nkd


def calc_ytw(
    settlement: date,
    maturity: date,
    offer_date: Optional[date],
    clean_price: float,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
    coupon_type: str,
    next_coupon_str: Optional[str] = None,
) -> Tuple[Optional[float], float]:
    """
    Возвращает (ytw, nkd).
    ytw — доходность к худшему (минимум YTM / YTC).
    nkd — накопленный купонный доход в рублях.
    """
    if not all([settlement, maturity, face_value, coupon_period]):
        return None, 0.0
    if maturity <= settlement:
        return None, 0.0
    if not clean_price or clean_price <= 0:
        return None, 0.0

    # Дисконтные облигации — YTW = YTM
    if coupon_type == 'ZERO':
        years = _days_between(settlement, maturity) / DAYS_IN_YEAR
        if years > 0 and clean_price > 0:
            ytw = (face_value / clean_price) ** (1 / years) - 1
            return max(0.0, ytw), 0.0
        return None, 0.0

    # Флоатеры — не считаем
    if coupon_type in ('FLOAT', 'VARIABLE'):
        return None, 0.0

    nkd = calc_nkd(
        settlement, maturity, face_value,
        coupon_value, coupon_period, coupon_type, next_coupon_str
    )
    # dirty_price в рублях
    dirty = (clean_price / 100.0) * face_value + nkd

    ytm = calc_ytm_newton(
        settlement, maturity, dirty,
        face_value, coupon_value, coupon_period, coupon_type
    )

    ytc = None
    if offer_date and offer_date > settlement:
        ytc = calc_ytc(
            settlement, offer_date, dirty,
            face_value, coupon_value, coupon_period, coupon_type
        )

    if ytm is None:
        ytw = ytc
    elif ytc is None:
        ytw = ytm
    else:
        ytw = min(ytm, ytc)

    return ytw, nkd


def calc_simple_yield(
    settlement: date,
    maturity: date,
    full_price_rub: float,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
) -> Optional[float]:
    """
    Простая доходность к погашению (без реинвестирования).
    = (Σкупонов + номинал - полная_цена) / полная_цена / лет * 100%
    """
    if not all([settlement, maturity, full_price_rub, face_value, coupon_period]):
        return None
    if full_price_rub <= 0 or maturity <= settlement:
        return None

    days_left = _days_between(settlement, maturity)
    years_left = days_left / DAYS_IN_YEAR
    if years_left <= 0:
        return None

    n_coupons = days_left / coupon_period if coupon_period > 0 else 0
    total_coupons = coupon_value * n_coupons
    profit = total_coupons + face_value - full_price_rub
    return round(profit / full_price_rub / years_left * 100, 4)


def calc_duration(
    settlement: date,
    maturity: date,
    face_value: float,
    coupon_value: float,
    coupon_period: int,
    ytm: float,
) -> Optional[float]:
    """Дюрация Маколея в днях."""
    if not all([settlement, maturity, face_value, coupon_period, ytm]):
        return None
    if ytm <= 0 or maturity <= settlement:
        return None

    coupon_days = coupon_period
    periods_per_year = DAYS_IN_YEAR / coupon_days
    r = ytm / periods_per_year
    days_total = _days_between(settlement, maturity)
    n_periods = periods_per_year * days_total / DAYS_IN_YEAR

    if n_periods <= 0 or abs(r) < 1e-10:
        return None

    weighted_pv = 0.0
    total_pv = 0.0
    for i in range(1, int(n_periods) + 1):
        t_years = i / periods_per_year
        pv = coupon_value / (1 + r) ** i
        weighted_pv += t_years * pv
        total_pv += pv

    pv_face = face_value / (1 + r) ** n_periods
    t_face = n_periods / periods_per_year
    weighted_pv += t_face * pv_face
    total_pv += pv_face

    if total_pv <= 0:
        return None

    duration_years = weighted_pv / total_pv
    return round(duration_years * DAYS_IN_YEAR, 2)


# ======================================================================
# ETL
# ======================================================================

class BondETL:
    DESCRIPTION_WORKERS = 10

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    def _parse_date(self, value) -> Optional[date]:
        if value is None:
            return None
        val_str = str(value).strip()
        if val_str in ('', '0000-00-00', 'null', 'None', 'NaN'):
            return None
        try:
            if 'T' in val_str:
                return datetime.fromisoformat(val_str.replace('Z', '+00:00')).date()
            return datetime.strptime(val_str, '%Y-%m-%d').date()
        except ValueError:
            return None

    def _safe_float(self, value, default: float = 0.0) -> float:
        if value in (None, ''):
            return default
        try:
            return float(str(value).replace(',', '.'))
        except ValueError:
            return default

    def _safe_int(self, value) -> Optional[int]:
        if value in (None, ''):
            return None
        try:
            return int(float(str(value).replace(',', '.')))
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Шаг 1: список облигаций
    # ------------------------------------------------------------------

    def fetch_bonds_list(self) -> Dict[str, Dict]:
        logger.info('Загрузка списка облигаций из MOEX ISS...')
        resp = self.session.get(
            'https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json',
            params={
                'iss.only': 'securities',
                'iss.meta': 'off',
                'securities.columns': (
                    'SECID,SHORTNAME,ISIN,FACEVALUE,'
                    'COUPONVALUE,COUPONPERIOD,BONDTYPE,'
                    'MATDATE,OFFERDATE'
                ),
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        cols = data.get('securities', {}).get('columns', [])
        rows = data.get('securities', {}).get('data', [])
        logger.info(f' Получено {len(rows)} строк.')

        raw: Dict[str, Dict] = {}
        for row in rows:
            d = dict(zip(cols, row))
            secid = d.get('SECID')
            if secid and secid not in raw:
                raw[secid] = d

        logger.info(f'Уникальных: {len(raw)}')
        return raw

    # ------------------------------------------------------------------
    # Шаг 2: description (параллельно)
    # ------------------------------------------------------------------

    def _fetch_description(self, secid: str) -> Dict:
        try:
            resp = self.session.get(
                f'https://iss.moex.com/iss/securities/{secid}.json',
                params={
                    'iss.only': 'description',
                    'iss.meta': 'off',
                    'description.columns': 'name,value',
                },
                timeout=10,
            )
            resp.raise_for_status()
            rows = resp.json().get('description', {}).get('data', [])
            fields = {r[0]: r[1] for r in rows if len(r) >= 2}

            full_name = fields.get('NAME') or fields.get('SHORTNAME')

            bond_type_desc = str(fields.get('BOND_TYPE') or '').lower()
            if any(k in bond_type_desc for k in ('флоатер', 'перемен', 'плавающ')):
                coupon_type = 'FLOAT'
            elif any(k in bond_type_desc for k in ('дисконт', 'zero', 'нулев')):
                coupon_type = 'ZERO'
            elif 'индекс' in bond_type_desc:
                coupon_type = 'INDEXED'
            else:
                coupon_type = None

            cper: Optional[int] = None
            if fields.get('COUPONFREQUENCY') not in (None, ''):
                try:
                    freq = int(fields['COUPONFREQUENCY'])
                    if freq > 0:
                        cper = round(DAYS_IN_YEAR / freq)
                except (ValueError, ZeroDivisionError):
                    pass

            return {
                'secid':                   secid,
                'name':                    full_name,
                'issuer':                  parse_issuer(full_name),
                'issue_date':              (
                    self._parse_date(fields.get('STARTDATEMOEX'))
                    or self._parse_date(fields.get('ISSUEDATE'))
                ),
                'has_amortization':        fields.get('AMORTIZATION') == '1',
                'coupon_type_override':    coupon_type,
                'coupon_value':            self._safe_float(fields.get('COUPONVALUE')),
                'coupon_period_from_freq': cper,
                'initial_face_value':      self._safe_float(
                    fields.get('INITIALFACEVALUE'), default=1000.0
                ),
                'coupon_percent':          self._safe_float(fields.get('COUPONPERCENT')),
                'list_level':              self._safe_int(fields.get('LISTLEVEL')),
                'is_qualified':            fields.get('ISQUALIFIEDINVESTORS') == '1',
                'issue_size':              self._safe_float(fields.get('ISSUESIZE')),
                # Дата следующего купона — нужна для точного расчёта НКД
                'next_coupon_date':        fields.get('COUPONDATE'),
            }
        except Exception as e:
            logger.debug(f'description для {secid}: {e}')
            return {
                'secid': secid, 'name': None, 'issuer': None,
                'issue_date': None, 'has_amortization': False,
                'coupon_type_override': None, 'coupon_value': 0.0,
                'coupon_period_from_freq': None, 'initial_face_value': 1000.0,
                'coupon_percent': 0.0, 'list_level': None,
                'is_qualified': False, 'issue_size': 0.0,
                'next_coupon_date': None,
            }

    def fetch_descriptions_parallel(self, secids: List[str]) -> Dict[str, Dict]:
        logger.info(
            f'Загрузка description для {len(secids)} облигаций '
            f'({self.DESCRIPTION_WORKERS} потоков)'
        )
        result: Dict[str, Dict] = {}
        done = 0
        with ThreadPoolExecutor(max_workers=self.DESCRIPTION_WORKERS) as executor:
            futures = {
                executor.submit(self._fetch_description, sid): sid
                for sid in secids
            }
            for future in as_completed(futures):
                desc = future.result()
                result[desc['secid']] = desc
                done += 1
                if done % 500 == 0:
                    logger.info(f'  → обработано {done}/{len(secids)}')
        logger.info(f' Description загружены: {len(result)} записей.')
        return result

    # ------------------------------------------------------------------
    # Шаг 3: рыночные данные
    # ------------------------------------------------------------------

    def fetch_market_data(self) -> Dict[str, Dict]:
        logger.info(' Загрузка рыночных данных')
        resp = self.session.get(
            'https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json',
            params={
                'iss.only': 'marketdata',
                'iss.meta': 'off',
                'marketdata.columns': 'SECID,LAST,YIELD,DURATION,ACCRUEDINT,VOLTODAY',
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        cols = data.get('marketdata', {}).get('columns', [])
        rows = data.get('marketdata', {}).get('data', [])

        result: Dict[str, Dict] = {}
        for row in rows:
            d = dict(zip(cols, row))
            secid = d.get('SECID')
            if secid:
                result[secid] = d

        filled = sum(1 for d in result.values() if d.get('YIELD') is not None)
        logger.info(f' Рыночные данные: {len(result)}, с доходностью: {filled}.')
        return result

    # ------------------------------------------------------------------
    # Сборка
    # ------------------------------------------------------------------

    def get_moex_bonds(self):
        raw          = self.fetch_bonds_list()
        descriptions = self.fetch_descriptions_parallel(list(raw.keys()))
        market_data  = self.fetch_market_data()

        bonds  = []
        prices = []
        today  = date.today()

        for secid, d in raw.items():
            desc = descriptions.get(secid, {})
            md   = market_data.get(secid, {})

            # --- Тип купона ---
            ctype = desc.get('coupon_type_override')
            if not ctype:
                bond_type_raw = str(d.get('BONDTYPE') or '').lower()
                if any(k in bond_type_raw for k in ('перемен', 'плавающ')):
                    ctype = 'FLOAT'
                elif any(k in bond_type_raw for k in ('дисконт', 'zero')):
                    ctype = 'ZERO'
                elif 'индекс' in bond_type_raw:
                    ctype = 'INDEXED'
                else:
                    ctype = 'FIXED'

            # --- Купон ---
            cval = 0.0 if ctype == 'ZERO' else (
                desc.get('coupon_value')
                or self._safe_float(d.get('COUPONVALUE'))
            )

            # --- Период купона ---
            cper: Optional[int] = None
            cper_raw = d.get('COUPONPERIOD')
            if cper_raw not in (None, ''):
                try:
                    cper = int(float(str(cper_raw).replace(',', '.')))
                except ValueError:
                    pass
            if cper is None:
                cper = desc.get('coupon_period_from_freq')

            face_value    = self._safe_float(d.get('FACEVALUE'), default=1000.0)
            maturity      = self._parse_date(d.get('MATDATE'))
            next_coupon   = desc.get('next_coupon_date')

            bonds.append({
                'secid':              secid,
                'isin':               d.get('ISIN'),
                'name':               desc.get('name') or d.get('SHORTNAME'),
                'issuer':             desc.get('issuer'),
                'face_value':         face_value,
                'initial_face_value': desc.get('initial_face_value', 1000.0),
                'currency':           'RUB',
                'coupon_type':        ctype,
                'coupon_value':       cval,
                'coupon_percent':     desc.get('coupon_percent', 0.0),
                'coupon_period':      cper,
                'maturity_date':      maturity,
                'issue_date':         desc.get('issue_date'),
                'list_level':         desc.get('list_level'),
                'is_qualified':       desc.get('is_qualified', False),
                'issue_size':         desc.get('issue_size', 0.0),
                'has_offer':          d.get('OFFERDATE') not in (None, ''),
                'has_amortization':   (
                    desc.get('has_amortization', False)
                    or 'аморт' in str(d.get('BONDTYPE') or '').lower()
                ),
            })

            # --- Рыночные данные + расчёты ---
            clean_price = self._safe_float(md.get('LAST')) or None
            volume      = self._safe_float(md.get('VOLTODAY')) or None

            # YTM и НКД
            ytm_calc, nkd_calc = (None, 0.0)
            if clean_price and maturity and cper:
                ytm_calc, nkd_calc = calc_ytm(
                    settlement=today,
                    maturity=maturity,
                    clean_price=clean_price,
                    face_value=face_value,
                    coupon_value=cval,
                    coupon_period=cper,
                    coupon_type=ctype,
                    next_coupon_str=next_coupon,
                )

            # YTW — доходность к худшему
            ytw_calc: Optional[float] = None
            if clean_price and maturity and cper:
                ytw_calc, _ = calc_ytw(
                    settlement=today,
                    maturity=maturity,
                    offer_date=self._parse_date(d.get('OFFERDATE')),
                    clean_price=clean_price,
                    face_value=face_value,
                    coupon_value=cval,
                    coupon_period=cper,
                    coupon_type=ctype,
                    next_coupon_str=next_coupon,
                )

            # Полная цена в рублях
            full_price_rub = None
            if clean_price is not None:
                full_price_rub = round(
                    clean_price / 100.0 * face_value + (nkd_calc or 0.0), 4
                )

            # Простая доходность
            simple_yield = None
            if full_price_rub and maturity and cper and cval:
                simple_yield = calc_simple_yield(
                    settlement=today,
                    maturity=maturity,
                    full_price_rub=full_price_rub,
                    face_value=face_value,
                    coupon_value=cval,
                    coupon_period=cper,
                )

            # Дюрация Маколея
            duration_calc = None
            if ytm_calc and maturity and cper and cval:
                duration_calc = calc_duration(
                    settlement=today,
                    maturity=maturity,
                    face_value=face_value,
                    coupon_value=cval,
                    coupon_period=cper,
                    ytm=ytm_calc,
                )

            # YTM из API как fallback если наш не посчитался
            ytm_api = self._safe_float(md.get('YIELD')) or None
            ytm_final = ytm_calc  # предпочитаем свой расчёт
            # ytm из API в процентах, переводим в доли
            if ytm_final is None and ytm_api is not None:
                ytm_final = ytm_api / 100.0

            if any(v is not None for v in (clean_price, ytm_final, nkd_calc or None)):
                # Расчёт надёжности
                list_level = desc.get('list_level')
                is_qualified = desc.get('is_qualified', False)
                has_offer = d.get('OFFERDATE') not in (None, '')
                issue_size = desc.get('issue_size', 0.0)

                reliability_score = calc_reliability_score(
                    list_level=list_level,
                    volume=volume,
                    duration=duration_calc,
                    issue_size=issue_size,
                    face_value=face_value,
                    is_qualified=is_qualified,
                    close_price=clean_price,
                    coupon_type=ctype,
                    has_offer=has_offer,
                )

                # Проверка на мусорную облигацию
                days_to_maturity = None
                if maturity:
                    days_to_maturity = (maturity - today).days

                is_junk, junk_reason = is_junk_bond(
                    list_level=list_level,
                    volume=volume,
                    reliability_score=reliability_score,
                    close_price=clean_price,
                    is_qualified=is_qualified,
                    maturity=maturity,
                    days_to_maturity=days_to_maturity,
                )

                prices.append({
                    'secid':         secid,
                    'price_date':    today,
                    'close_price':   clean_price,
                    'yield':         round(ytm_final * 100, 6) if ytm_final else None,
                    'yield_simple':  simple_yield,
                    'ytw':           round(ytw_calc * 100, 6) if ytw_calc else None,
                    'duration':      duration_calc,
                    'nkd':           nkd_calc if nkd_calc else None,
                    'full_price_rub': full_price_rub,
                    'volume':        volume,
                    'reliability_score': reliability_score,
                    'is_junk':       is_junk,
                    'junk_reason':   junk_reason if is_junk else None,
                })

        return bonds, prices

    # ------------------------------------------------------------------
    # ETL
    # ------------------------------------------------------------------

    def _create_tables(self, cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bonds (
            id                 SERIAL PRIMARY KEY,
            secid              TEXT UNIQUE NOT NULL,
            isin               TEXT,
            name               TEXT,
            issuer             TEXT,
            face_value         NUMERIC,
            initial_face_value NUMERIC DEFAULT 1000,
            currency           TEXT,
            coupon_type        TEXT,
            coupon_value       NUMERIC,
            coupon_percent     NUMERIC,
            coupon_period      INT,
            maturity_date      DATE,
            issue_date         DATE,
            list_level         SMALLINT,
            is_qualified       BOOLEAN DEFAULT FALSE,
            issue_size         NUMERIC,
            has_offer          BOOLEAN,
            has_amortization   BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS bond_prices (
            id                SERIAL PRIMARY KEY,
            secid             TEXT NOT NULL REFERENCES bonds(secid) ON DELETE CASCADE,
            price_date        DATE NOT NULL,
            close_price       NUMERIC,   -- чистая цена, % от номинала
            yield             NUMERIC,   -- YTM в % годовых (наш расчёт или API)
            yield_simple      NUMERIC,   -- простая доходность, % годовых
            ytw               NUMERIC,   -- Yield to Worst, % годовых
            duration          NUMERIC,   -- дюрация Маколея, дней
            nkd               NUMERIC,   -- НКД, руб.
            full_price_rub    NUMERIC,   -- полная цена покупки, руб.
            volume            NUMERIC,   -- объём торгов за день
            reliability_score NUMERIC,   -- оценка надёжности (0-100)
            is_junk           BOOLEAN,  -- признак мусорной облигации
            junk_reason       TEXT,      -- причина отнесения к мусору
            UNIQUE(secid, price_date)
        );

        CREATE INDEX IF NOT EXISTS idx_bond_prices_secid_date
            ON bond_prices(secid, price_date DESC);
        """)

    def run_etl(self):
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                self._create_tables(cur)
            conn.commit()
        logger.info(' Таблицы готовы.')

        bonds, prices = self.get_moex_bonds()

        if not bonds:
            logger.warning(' Не получено данных из MOEX.')
            return

        logger.info(f' Облигаций: {len(bonds)}, ценовых записей: {len(prices)}')
        logger.info(f' issuer:         {sum(1 for b in bonds if b["issuer"])}/{len(bonds)}')
        logger.info(f' issue_date:     {sum(1 for b in bonds if b["issue_date"])}/{len(bonds)}')
        logger.info(f' coupon_percent: {sum(1 for b in bonds if b["coupon_percent"])}/{len(bonds)}')
        logger.info(f' ytm заполнен:   {sum(1 for p in prices if p["yield"])}/{len(prices)}')
        logger.info(f' ytw заполнен:   {sum(1 for p in prices if p.get("ytw"))}/{len(prices)}')
        logger.info(f' nkd заполнен:   {sum(1 for p in prices if p["nkd"])}/{len(prices)}')
        junk_count = sum(1 for p in prices if p.get("is_junk"))
        logger.info(f' мусорных облигаций: {junk_count}/{len(prices)}')
        scored = [p for p in prices if p.get("reliability_score") is not None]
        if scored:
            avg_score = sum(p["reliability_score"] for p in scored) / len(scored)
            high_risk = sum(1 for p in scored if p["reliability_score"] < 30)
            logger.info(f' avg reliability_score: {avg_score:.1f}, высокий риск (<30): {high_risk}')

        samples = [(b['name'], b['issuer']) for b in bonds if b['issuer']][:5]
        for name, issuer in samples:
            logger.info(f' «{name}» → «{issuer}»')

        bonds_query = """
            INSERT INTO bonds (
                secid, isin, name, issuer,
                face_value, initial_face_value, currency,
                coupon_type, coupon_value, coupon_percent, coupon_period,
                maturity_date, issue_date,
                list_level, is_qualified, issue_size,
                has_offer, has_amortization
            ) VALUES %s
            ON CONFLICT (secid) DO UPDATE SET
                isin               = EXCLUDED.isin,
                name               = EXCLUDED.name,
                issuer             = EXCLUDED.issuer,
                face_value         = EXCLUDED.face_value,
                initial_face_value = EXCLUDED.initial_face_value,
                currency           = EXCLUDED.currency,
                coupon_type        = EXCLUDED.coupon_type,
                coupon_value       = EXCLUDED.coupon_value,
                coupon_percent     = EXCLUDED.coupon_percent,
                coupon_period      = EXCLUDED.coupon_period,
                maturity_date      = EXCLUDED.maturity_date,
                issue_date         = EXCLUDED.issue_date,
                list_level         = EXCLUDED.list_level,
                is_qualified       = EXCLUDED.is_qualified,
                issue_size         = EXCLUDED.issue_size,
                has_offer          = EXCLUDED.has_offer,
                has_amortization   = EXCLUDED.has_amortization
        """

        prices_query = """
            INSERT INTO bond_prices (
                secid, price_date, close_price,
                yield, yield_simple, ytw,
                duration, nkd, full_price_rub, volume,
                reliability_score, is_junk, junk_reason
            ) VALUES %s
            ON CONFLICT (secid, price_date) DO UPDATE SET
                close_price       = EXCLUDED.close_price,
                yield             = EXCLUDED.yield,
                yield_simple      = EXCLUDED.yield_simple,
                ytw               = EXCLUDED.ytw,
                duration          = EXCLUDED.duration,
                nkd               = EXCLUDED.nkd,
                full_price_rub    = EXCLUDED.full_price_rub,
                volume            = EXCLUDED.volume,
                reliability_score = EXCLUDED.reliability_score,
                is_junk           = EXCLUDED.is_junk,
                junk_reason       = EXCLUDED.junk_reason
        """

        bonds_values = [
            (
                b['secid'], b['isin'], b['name'], b['issuer'],
                b['face_value'], b['initial_face_value'], b['currency'],
                b['coupon_type'], b['coupon_value'], b['coupon_percent'], b['coupon_period'],
                b['maturity_date'], b['issue_date'],
                b['list_level'], b['is_qualified'], b['issue_size'],
                b['has_offer'], b['has_amortization']
            )
            for b in bonds
        ]

        prices_values = [
            (
                p['secid'], p['price_date'], p['close_price'],
                p['yield'], p['yield_simple'], p.get('ytw'),
                p['duration'], p['nkd'], p['full_price_rub'], p['volume'],
                p.get('reliability_score'), p.get('is_junk', False), p.get('junk_reason')
            )
            for p in prices
        ]

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                from psycopg2.extras import execute_values
                execute_values(cur, bonds_query, bonds_values, page_size=500)
                logger.info(' bonds сохранены.')

                if prices_values:
                    execute_values(cur, prices_query, prices_values, page_size=500)
                    logger.info(f' bond_prices сохранены: {len(prices_values)} записей.')

            conn.commit()

        logger.info('База данных успешно обновлена.')


if __name__ == '__main__':
    try:
        etl = BondETL()
        etl.run_etl()
    except Exception as e:
        logger.error(f'Critical Error: {e}', exc_info=True)