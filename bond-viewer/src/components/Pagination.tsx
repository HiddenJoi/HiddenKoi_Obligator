
interface PaginationProps {
  currentPage: number
  totalPages: number
  onPageChange: (page: number) => void
}

function getPageNumbers(currentPage: number, totalPages: number): (number | "...")[] {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, i) => i)
  }

  const pages: (number | "...")[] = []
  
  if (currentPage <= 4) {
    pages.push(0, 1, 2, 3, 4, "...", totalPages - 1)
  } else if (currentPage >= totalPages - 5) {
    pages.push(0, "...", totalPages - 5, totalPages - 4, totalPages - 3, totalPages - 2, totalPages - 1)
  } else {
    pages.push(0, "...", currentPage - 1, currentPage, currentPage + 1, "...", totalPages - 1)
  }
  
  return pages
}

export function Pagination({ currentPage, totalPages, onPageChange }: PaginationProps) {
  if (totalPages <= 1) return null

  const pageNumbers = getPageNumbers(currentPage, totalPages)

  return (
    <div className="flex items-center gap-2">
      {/* First page button */}
      <button
        onClick={() => onPageChange(0)}
        disabled={currentPage === 0}
        className="px-2 py-1 text-sm rounded border border-slate-300 hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        title="Первая страница"
      >
        ««
      </button>

      {/* Previous button */}
      <button
        onClick={() => onPageChange(currentPage - 1)}
        disabled={currentPage === 0}
        className="px-3 py-1 text-sm rounded border border-slate-300 hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
      >
        ← Назад
      </button>

      {/* Page numbers */}
      {pageNumbers.map((page, index) =>
        page === "..." ? (
          <span key={`ellipsis-${index}`} className="px-1 py-1 text-slate-400">
            ...
          </span>
        ) : (
          <button
            key={page}
            onClick={() => onPageChange(page as number)}
            className={[
              "min-w-[36px] px-2 py-1 text-sm rounded border transition-colors",
              currentPage === page
                ? "bg-blue-600 text-white border-blue-600"
                : "border-slate-300 hover:bg-slate-100",
            ].join(" ")}
          >
            {(page as number) + 1}
          </button>
        )
      )}

      {/* Next button */}
      <button
        onClick={() => onPageChange(currentPage + 1)}
        disabled={currentPage === totalPages - 1}
        className="px-3 py-1 text-sm rounded border border-slate-300 hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
      >
        Вперед →
      </button>

      {/* Last page button */}
      <button
        onClick={() => onPageChange(totalPages - 1)}
        disabled={currentPage === totalPages - 1}
        className="px-2 py-1 text-sm rounded border border-slate-300 hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        title="Последняя страница"
      >
        »»
      </button>
    </div>
  )
}

interface PaginationInfoProps {
  from: number
  to: number
  total: number
}

export function PaginationInfo({ from, to, total }: PaginationInfoProps) {
  return (
    <span className="text-sm text-slate-600">
      Показано <strong className="text-slate-800">{from}–{to}</strong> из{" "}
      <strong className="text-slate-800">{total}</strong> облигаций
    </span>
  )
}

interface LimitSelectProps {
  value: number
  onChange: (limit: number) => void
  options?: number[]
}

const DEFAULT_LIMITS = [10, 25, 50, 100]

export function LimitSelect({ value, onChange, options = DEFAULT_LIMITS }: LimitSelectProps) {
  return (
    <div className="flex items-center gap-2">
      <label className="text-sm text-slate-600">На странице:</label>
      <select
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="border border-slate-300 rounded-lg px-2 py-1.5 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
      >
        {options.map((opt) => (
          <option key={opt} value={opt}>
            {opt}
          </option>
        ))}
      </select>
    </div>
  )
}
