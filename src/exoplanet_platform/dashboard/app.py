"""Streamlit dashboard for the Exoplanet Analysis Platform.

This is a single-file multi-page app with a sidebar navigation. Heavy
imports (streamlit, plotly, lightkurve) are kept at module or function
scope intentionally — the file is only ever run by streamlit.

Pages
-----
- Overview: DB-wide metrics and distribution plots.
- Planet Browser: filter + paginated dataframe of stored planets.
- Light Curve Viewer: download or fetch-from-DB and plot flux / phase fold.
- Transit Search: run BLS on an arbitrary target and inspect signals.
- Habitability: assess HZ membership and visualise orbit geometry.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from exoplanet_platform.dashboard.components import (
    hz_diagram,
    light_curve_plot,
    periodogram_plot,
    planet_card,
)
from exoplanet_platform.domain import Catalog, Planet, Star
from exoplanet_platform.exceptions import ExoplanetPlatformError
from exoplanet_platform.logging_config import configure_logging, get_logger

configure_logging(level="INFO", json_output=False)
logger = get_logger(__name__)


# --------------------------------------------------------------------------- #
# Cached data access
# --------------------------------------------------------------------------- #


@st.cache_data(ttl=3600)
def _load_planets(
    host_star: Optional[str] = None,
    catalog: Optional[str] = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Load planets from the local DB as a list of plain dicts (cache-friendly)."""
    from exoplanet_platform.storage import PlanetRepository, get_session

    cat_enum: Optional[Catalog] = None
    if catalog:
        try:
            cat_enum = Catalog(catalog)
        except ValueError:
            cat_enum = None
    with get_session() as s:
        repo = PlanetRepository(s)
        planets = repo.list(host_star=host_star, catalog=cat_enum, limit=limit)
    return [p.model_dump(mode="json") for p in planets]


@st.cache_data(ttl=3600)
def _count_rows() -> dict[str, int]:
    """Return headline row counts for the overview page."""
    from exoplanet_platform.storage import (
        LightCurveRepository,
        PlanetRepository,
        StarRepository,
        get_session,
    )

    out = {"planets": 0, "stars": 0, "light_curves": 0, "recent_planets": 0}
    with get_session() as s:
        try:
            out["planets"] = PlanetRepository(s).count()
        except Exception:  # noqa: BLE001
            pass
        try:
            out["stars"] = StarRepository(s).count()
        except Exception:  # noqa: BLE001
            pass
        try:
            out["light_curves"] = LightCurveRepository(s).count()
        except Exception:  # noqa: BLE001
            pass
        try:
            out["recent_planets"] = PlanetRepository(s).count_recent(days=7)
        except Exception:  # noqa: BLE001
            pass
    return out


@st.cache_data(ttl=3600)
def _get_star(identifier: str) -> Optional[dict[str, Any]]:
    """Fetch a single star dict from the DB by identifier, or None."""
    from exoplanet_platform.storage import StarRepository, get_session

    with get_session() as s:
        star = StarRepository(s).get(identifier)
    return star.model_dump(mode="json") if star else None


def _planets_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Turn a list of planet dicts into a tidy :class:`pandas.DataFrame`."""
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _empty_db_banner() -> None:
    """Render the 'your DB is empty' call-to-action banner."""
    st.info(
        "No data in the database yet. "
        "Run `exoplanet db init` and then `exoplanet ingest <identifier>` "
        "to populate it — this page will refresh automatically."
    )


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #


def page_overview() -> None:
    """Render the Overview page: headline metrics and distribution plots."""
    st.title("Overview")
    counts = _count_rows()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Planets (DB)", counts["planets"])
    c2.metric("Stars (DB)", counts["stars"])
    c3.metric("Light curves", counts["light_curves"])
    c4.metric("Ingested (7d)", counts["recent_planets"])

    rows = _load_planets(limit=5000)
    if not rows:
        _empty_db_banner()
        return

    df = _planets_df(rows)
    st.subheader("Distribution of planet radii")
    if "radius_earth" in df.columns and df["radius_earth"].notna().any():
        fig = px.histogram(
            df.dropna(subset=["radius_earth"]),
            x="radius_earth",
            nbins=50,
            labels={"radius_earth": "Radius (R⊕)"},
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No radius data available.")

    st.subheader("Mass vs radius")
    scatter_df = df.dropna(subset=["mass_earth", "radius_earth"])
    if not scatter_df.empty:
        fig = px.scatter(
            scatter_df,
            x="mass_earth",
            y="radius_earth",
            color="equilibrium_temperature_k",
            hover_name="identifier",
            labels={
                "mass_earth": "Mass (M⊕)",
                "radius_earth": "Radius (R⊕)",
                "equilibrium_temperature_k": "T_eq (K)",
            },
            log_x=True,
            log_y=True,
            color_continuous_scale="Turbo",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("Not enough mass+radius data for a scatter plot.")


def page_planet_browser() -> None:
    """Render the Planet Browser page: filter controls + paginated table + details."""
    st.title("Planet Browser")

    rows = _load_planets(limit=5000)
    if not rows:
        _empty_db_banner()
        return
    df = _planets_df(rows)

    with st.sidebar:
        st.header("Filters")
        host_star = st.text_input("Host star contains")
        catalogs = sorted([c for c in df["catalog"].dropna().unique()]) if "catalog" in df else []
        catalog = st.selectbox("Catalog", ["(any)", *catalogs])
        radius_min, radius_max = st.slider(
            "Radius (R⊕)", 0.0, 30.0, (0.0, 30.0), step=0.5
        )
        period_min, period_max = st.slider(
            "Period (days)", 0.0, 2000.0, (0.0, 2000.0), step=1.0
        )
        page_size = st.number_input("Page size", min_value=10, max_value=500, value=50)

    filt = df.copy()
    if host_star:
        filt = filt[
            filt["host_star"].astype(str).str.contains(host_star, case=False, na=False)
        ]
    if catalog != "(any)":
        filt = filt[filt["catalog"] == catalog]
    if "radius_earth" in filt:
        filt = filt[
            (filt["radius_earth"].fillna(-1) >= radius_min)
            & (filt["radius_earth"].fillna(1e9) <= radius_max)
        ]
    if "orbital_period_days" in filt:
        filt = filt[
            (filt["orbital_period_days"].fillna(-1) >= period_min)
            & (filt["orbital_period_days"].fillna(1e12) <= period_max)
        ]

    total = len(filt)
    st.caption(f"{total} planets after filtering.")
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1) - 1
    start = int(page * page_size)
    end = int(start + page_size)
    view = filt.iloc[start:end]
    selection = st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    # Handle row selection (streamlit >= 1.35 event model).
    selected_rows = []
    if isinstance(selection, dict):
        selected_rows = selection.get("selection", {}).get("rows", [])
    if selected_rows:
        idx = selected_rows[0]
        if idx < len(view):
            try:
                planet = Planet(**view.iloc[idx].to_dict())
                planet_card(planet)
                with st.expander("All fields"):
                    st.json(planet.model_dump(mode="json"))
            except Exception as e:  # noqa: BLE001
                st.error(f"Could not render selected planet: {e}")


def page_light_curve_viewer() -> None:
    """Render the Light Curve Viewer page: download + plot + optional phase fold."""
    st.title("Light Curve Viewer")
    target = st.text_input("Target", value="TIC 307210830")
    mission = st.selectbox("Mission", ["TESS", "Kepler", "K2"])
    sector = st.number_input(
        "Sector / quarter (0 = any)", min_value=0, max_value=999, value=0
    )
    fold_col1, fold_col2 = st.columns(2)
    fold_period = fold_col1.number_input("Fold period (days, 0 = disabled)", value=0.0)
    fold_epoch = fold_col2.number_input("Fold epoch (BJD)", value=0.0)

    if not st.button("Download / fetch light curve"):
        return

    from exoplanet_platform.ingestion.mast import MASTClient

    try:
        client = MASTClient()
        with st.spinner(f"Downloading light curve for {target}..."):
            lc = client.download_light_curve(
                target,
                mission=Catalog(mission.lower()),
                sector=int(sector) if sector else None,
            )
    except ExoplanetPlatformError as e:
        st.error(f"Light curve download failed: {e}")
        return

    st.success(f"Loaded {len(lc.time_bjd)} points for {lc.target}.")
    st.plotly_chart(light_curve_plot(lc), use_container_width=True)

    if fold_period > 0:
        from exoplanet_platform.analysis.transit import TransitDetector

        try:
            phase, flux = TransitDetector().phase_fold(lc, fold_period, fold_epoch)
        except ExoplanetPlatformError as e:
            st.error(f"Phase fold failed: {e}")
            return
        import plotly.graph_objects as go

        fig = go.Figure()
        fig.add_trace(
            go.Scattergl(
                x=phase,
                y=flux,
                mode="markers",
                marker={"size": 3, "opacity": 0.6, "color": "#2ca02c"},
            )
        )
        fig.update_layout(
            title=f"Phase fold at P={fold_period} d, T0={fold_epoch}",
            xaxis_title="Phase",
            yaxis_title="Flux",
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)


def page_transit_search() -> None:
    """Render the Transit Search page: BLS periodogram + top signals."""
    st.title("Transit Search")
    target = st.text_input("Target", value="TIC 307210830")
    mission = st.selectbox("Mission", ["TESS", "Kepler", "K2"], key="transit_mission")
    col1, col2 = st.columns(2)
    min_period = col1.number_input("Min period (days)", value=0.5, min_value=0.1)
    max_period = col2.number_input("Max period (days)", value=30.0, min_value=0.5)

    if not st.button("Run BLS search"):
        return

    from exoplanet_platform.analysis.transit import TransitDetector
    from exoplanet_platform.ingestion.mast import MASTClient

    try:
        client = MASTClient()
        with st.spinner(f"Downloading {mission} light curve..."):
            lc = client.download_light_curve(target, mission=Catalog(mission.lower()))
        detector = TransitDetector()
        with st.spinner("Detrending + BLS..."):
            detrended = detector.detrend(lc)
            signals = detector.search(
                detrended,
                min_period_days=float(min_period),
                max_period_days=float(max_period),
            )
    except ExoplanetPlatformError as e:
        st.error(f"Transit search failed: {e}")
        return

    if not signals:
        st.warning("No signals found.")
        return

    # Build a rough periodogram from the signal grid for display. If the
    # detector exposes full power arrays we'd use them here; we fall back to
    # the top signals sorted by period.
    periods = np.array([s.period_days for s in signals])
    powers = np.array([s.power for s in signals])
    order = np.argsort(periods)
    st.plotly_chart(
        periodogram_plot(periods[order], powers[order]), use_container_width=True
    )

    st.subheader("Top 5 BLS signals")
    df = pd.DataFrame([s.model_dump() for s in signals[:5]])
    st.dataframe(df, hide_index=True, use_container_width=True)

    best = signals[0]
    st.subheader(f"Best signal: P={best.period_days:.5f} d")
    try:
        phase, flux = TransitDetector().phase_fold(
            detrended, best.period_days, best.epoch_bjd
        )
    except ExoplanetPlatformError as e:
        st.error(f"Phase fold failed: {e}")
        return
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(
        go.Scattergl(
            x=phase,
            y=flux,
            mode="markers",
            marker={"size": 3, "opacity": 0.6, "color": "#1f77b4"},
        )
    )
    fig.update_layout(
        title="Phase-folded light curve (best signal)",
        xaxis_title="Phase",
        yaxis_title="Flux",
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)


def page_habitability() -> None:
    """Render the Habitability page: HZ diagram + ESI readout for a stored planet."""
    st.title("Habitability")

    rows = _load_planets(limit=5000)
    if not rows:
        _empty_db_banner()
        return

    ids = [r["identifier"] for r in rows]
    chosen = st.selectbox("Planet", ids)
    if not chosen:
        return
    planet_dict = next(r for r in rows if r["identifier"] == chosen)
    try:
        planet = Planet(**planet_dict)
    except Exception as e:  # noqa: BLE001
        st.error(f"Could not parse planet record: {e}")
        return

    star_dict = _get_star(planet.host_star)
    if star_dict is None:
        st.warning(
            f"Host star '{planet.host_star}' is not in the DB. "
            f"Run `exoplanet ingest {planet.host_star} --catalog gaia`."
        )
        return
    try:
        star = Star(**star_dict)
    except Exception as e:  # noqa: BLE001
        st.error(f"Could not parse star record: {e}")
        return

    from exoplanet_platform.analysis.habitability import HabitabilityAnalyzer

    try:
        analyzer = HabitabilityAnalyzer()
        assessment = analyzer.assess(planet, star)
    except ExoplanetPlatformError as e:
        st.error(f"Habitability analysis failed: {e}")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Conservative HZ",
        "YES" if assessment.in_conservative_hz else "NO",
    )
    c2.metric(
        "Optimistic HZ",
        "YES" if assessment.in_optimistic_hz else "NO",
    )
    c3.metric(
        "ESI",
        f"{assessment.earth_similarity_index:.3f}"
        if assessment.earth_similarity_index is not None
        else "—",
    )

    hz_bounds = (
        assessment.hz_inner_au * 0.9,
        assessment.hz_inner_au,
        assessment.hz_outer_au,
        assessment.hz_outer_au * 1.1,
    )
    st.plotly_chart(hz_diagram(star, [planet], hz_bounds), use_container_width=True)
    if assessment.notes:
        st.caption(assessment.notes)


# --------------------------------------------------------------------------- #
# App entry
# --------------------------------------------------------------------------- #


def main() -> None:
    """Wire up sidebar navigation and dispatch to the selected page."""
    st.set_page_config(
        page_title="Exoplanet Analysis Platform",
        page_icon=":telescope:",
        layout="wide",
    )
    with st.sidebar:
        st.title("Exoplanet Platform")
        page = st.radio(
            "Navigate",
            [
                "Overview",
                "Planet Browser",
                "Light Curve Viewer",
                "Transit Search",
                "Habitability",
            ],
        )

    if page == "Overview":
        page_overview()
    elif page == "Planet Browser":
        page_planet_browser()
    elif page == "Light Curve Viewer":
        page_light_curve_viewer()
    elif page == "Transit Search":
        page_transit_search()
    elif page == "Habitability":
        page_habitability()


main()
