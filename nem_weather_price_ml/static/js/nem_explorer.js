/**
 * Loads /api/features and drives Chart.js for the NEM + BOM explorer.
 */
(function () {
  const chartColors = {
    text: "#e8eaed",
    grid: "#2a2d36",
    accent: "#7a9eb5",
    green: "#33ff66",
  };

  function hubPalette() {
    return {
      color: chartColors.text,
      borderColor: chartColors.grid,
    };
  }

  function showError(msg) {
    const box = document.getElementById("nem-error");
    const m = document.getElementById("nem-error-msg");
    box.classList.add("is-visible");
    m.textContent = msg;
    document.getElementById("nem-main").style.display = "none";
  }

  function baseChartOptions() {
    const p = hubPalette();
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: p.color } },
      },
      scales: {
        x: {
          ticks: { color: p.color, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
          grid: { color: p.borderColor },
        },
        y: {
          ticks: { color: p.color },
          grid: { color: p.borderColor },
        },
      },
    };
  }

  let chartRrp = null;
  let chartTmax = null;
  let chartScatter = null;

  function destroyCharts() {
    [chartRrp, chartTmax, chartScatter].forEach((c) => {
      if (c) c.destroy();
    });
    chartRrp = chartTmax = chartScatter = null;
  }

  function renderCharts(rows, region) {
    const subset = rows.filter((r) => r.region === region).sort((a, b) => a.date.localeCompare(b.date));
    const labels = subset.map((r) => r.date);
    const rrp = subset.map((r) => (r.rrp_avg == null ? null : Number(r.rrp_avg)));
    const tmax = subset.map((r) => (r.tmax_c == null ? null : Number(r.tmax_c)));

    destroyCharts();

    const ctxR = document.getElementById("chart-rrp");
    chartRrp = new Chart(ctxR, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "RRP avg",
            data: rrp,
            borderColor: chartColors.accent,
            backgroundColor: "rgba(122, 158, 181, 0.12)",
            tension: 0.15,
            spanGaps: true,
          },
        ],
      },
      options: baseChartOptions(),
    });

    const ctxT = document.getElementById("chart-tmax");
    chartTmax = new Chart(ctxT, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Tmax",
            data: tmax,
            borderColor: chartColors.green,
            backgroundColor: "rgba(51, 255, 102, 0.08)",
            tension: 0.15,
            spanGaps: true,
          },
        ],
      },
      options: baseChartOptions(),
    });

    const sc = subset.filter((r) => r.tmax_c != null && r.rrp_avg != null);
    chartScatter = new Chart(document.getElementById("chart-scatter"), {
      type: "scatter",
      data: {
        datasets: [
          {
            label: "Days",
            data: sc.map((r) => ({ x: Number(r.tmax_c), y: Number(r.rrp_avg) })),
            backgroundColor: "rgba(122, 158, 181, 0.55)",
            borderColor: chartColors.accent,
            pointRadius: 4,
            pointHoverRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: chartColors.text } },
        },
        scales: {
          x: {
            title: { display: true, text: "Tmax (°C)", color: chartColors.text },
            ticks: { color: chartColors.text },
            grid: { color: chartColors.grid },
          },
          y: {
            title: { display: true, text: "RRP avg ($/MWh)", color: chartColors.text },
            ticks: { color: chartColors.text },
            grid: { color: chartColors.grid },
          },
        },
      },
    });

    const miss = subset.filter((r) => r.weather_missing).length;
    const d0 = labels[0];
    const d1 = labels[labels.length - 1];
    document.getElementById("nem-summary").textContent =
      `${subset.length} days (${d0} → ${d1}). BOM gaps flagged: ${miss}.`;
  }

  async function init() {
    const res = await fetch("/api/features");
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      showError(data.error || "HTTP " + res.status);
      return;
    }
    const rows = data.rows || [];
    if (!rows.length) {
      showError("No rows in dataset.");
      return;
    }

    const regions = [...new Set(rows.map((r) => r.region).filter(Boolean))].sort();
    const sel = document.getElementById("nem-region");
    sel.innerHTML = "";
    regions.forEach((r) => {
      const o = document.createElement("option");
      o.value = r;
      o.textContent = r;
      sel.appendChild(o);
    });

    sel.addEventListener("change", () => renderCharts(rows, sel.value));
    renderCharts(rows, regions[0]);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
