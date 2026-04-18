export function formatYield(value: number | null | undefined): string {
  if (value == null) return "—"
  return value.toFixed(2) + "%"
}

export function formatDuration(value: number | null | undefined): string {
  if (value == null) return "—"
  return Math.round(value) + " дн"
}

export function formatNkd(value: number | null | undefined): string {
  if (value == null) return "—"
  return value.toFixed(2) + " ₽"
}

export function formatPrice(value: number | null | undefined): string {
  if (value == null) return "—"
  return value.toFixed(2) + "%"
}

export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—"
  return new Date(iso).toLocaleDateString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  })
}
