"""Typer-based command-line interface for the Exoplanet Analysis Platform.

This module wires together every sub-system (ingestion, analysis, storage)
behind a friendly ``exoplanet`` entry-point. Heavy imports (``lightkurve``,
``streamlit``, ``uvicorn``) are deferred to command bodies so that
``exoplanet --help`` stays fast.

Command tree
------------
::

    exoplanet search <name>
    exoplanet get <identifier>
    exoplanet ingest <identifier> [--catalog nasa|gaia]
    exoplanet list-planets [--host-star X] [--catalog C] [--limit N]
    exoplanet orbit <body> --start ... --stop ... [--step 1d]
    exoplanet light-curve download <target> --mission ... [--sector N]
    exoplanet analyze transit <target> --mission ... [--min-period 0.5]
    exoplanet analyze habitability <planet>
    exoplanet db init
    exoplanet serve api [--host] [--port] [--reload]
    exoplanet serve dashboard [--port 8501]
"""

from __future__ import annotations

import csv
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from exoplanet_platform.config import get_settings
from exoplanet_platform.domain import Catalog, Planet
from exoplanet_platform.exceptions import ExoplanetPlatformError
from exoplanet_platform.logging_config import configure_logging, get_logger

console = Console()
logger = get_logger(__name__)


app = typer.Typer(
    help="Exoplanet Analysis Platform CLI",
    no_args_is_help=True,
    add_completion=False,
)
analyze = typer.Typer(help="Run analysis pipelines on light curves and planets.")
serve = typer.Typer(help="Run long-lived servers (API, dashboard).")
light_curve = typer.Typer(name="light-curve", help="Download and manage light curves.")
db = typer.Typer(help="Database administrative commands.")

app.add_typer(analyze, name="analyze")
app.add_typer(serve, name="serve")
app.add_typer(light_curve, name="light-curve")
app.add_typer(db, name="db")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _die(message: str, exc: BaseException | None = None) -> None:
    """Print a red error message and exit with code 1.

    Intended for handling ``ExoplanetPlatformError`` and similar recoverable
    failures at the top of each command body.
    """
    text = Text(message, style="bold red")
    console.print(text)
    if exc is not None:
        logger.error("cli.error", message=message, exc=str(exc))
    raise typer.Exit(code=1)


def _planet_table(planets: list[Planet], title: str = "Planets") -> Table:
    """Build a rich Table summarising a list of planets."""
    table = Table(title=title, header_style="bold cyan", expand=True)
    table.add_column("Identifier", style="bold")
    table.add_column("Host Star")
    table.add_column("Method")
    table.add_column("Period (d)", justify="right")
    table.add_column("Radius (R⊕)", justify="right")
    table.add_column("Mass (M⊕)", justify="right")
    table.add_column("Teq (K)", justify="right")
    table.add_column("Catalog")
    for p in planets:
        table.add_row(
            p.identifier,
            p.host_star,
            str(p.discovery_method.value if p.discovery_method else "-"),
            f"{p.orbital_period_days:.4f}" if p.orbital_period_days else "-",
            f"{p.radius_earth:.2f}" if p.radius_earth else "-",
            f"{p.mass_earth:.2f}" if p.mass_earth else "-",
            f"{p.equilibrium_temperature_k:.0f}" if p.equilibrium_temperature_k else "-",
            p.catalog.value if p.catalog else "-",
        )
    return table


def _planet_panel(p: Planet) -> Panel:
    """Render a single planet as a key-value Panel."""
    lines = [
        f"[bold cyan]Identifier[/bold cyan]:        {p.identifier}",
        f"[bold cyan]Host star[/bold cyan]:         {p.host_star}",
        f"[bold cyan]Catalog[/bold cyan]:           {p.catalog.value if p.catalog else '-'}",
        f"[bold cyan]Discovery method[/bold cyan]:  {p.discovery_method.value}",
        f"[bold cyan]Discovery year[/bold cyan]:    {p.discovery_year or '-'}",
        "",
        f"[bold]Orbital period[/bold]:     {p.orbital_period_days} d",
        f"[bold]Semi-major axis[/bold]:    {p.semi_major_axis_au} AU",
        f"[bold]Eccentricity[/bold]:       {p.eccentricity}",
        f"[bold]Inclination[/bold]:        {p.inclination_deg} deg",
        "",
        f"[bold]Radius[/bold]:             {p.radius_earth} R⊕",
        f"[bold]Mass[/bold]:               {p.mass_earth} M⊕",
        f"[bold]Density[/bold]:            {p.density_g_cm3} g/cm³",
        f"[bold]T_eq[/bold]:               {p.equilibrium_temperature_k} K",
        f"[bold]Insolation[/bold]:         {p.insolation_flux_earth} S⊕",
        "",
        f"[bold]Transit epoch[/bold]:      {p.transit_epoch_bjd} BJD",
        f"[bold]Transit duration[/bold]:   {p.transit_duration_hours} h",
        f"[bold]Transit depth[/bold]:      {p.transit_depth_ppm} ppm",
    ]
    return Panel("\n".join(lines), title=f"Planet {p.identifier}", border_style="cyan")


# --------------------------------------------------------------------------- #
# Global callback
# --------------------------------------------------------------------------- #


@app.callback()
def _main(
    log_level: str = typer.Option(
        "INFO", "--log-level", help="Root log level (DEBUG, INFO, WARNING, ERROR)."
    ),
    json_logs: bool = typer.Option(
        False, "--json-logs", help="Emit JSON log records instead of console format."
    ),
) -> None:
    """Configure structured logging before dispatching to the chosen command."""
    configure_logging(level=log_level, json_output=json_logs)
    # Touch settings so that the data_dir is created early.
    get_settings()


# --------------------------------------------------------------------------- #
# Search / Get / List / Ingest
# --------------------------------------------------------------------------- #


@app.command("search")
def search_cmd(
    name: str = typer.Argument(..., help="Substring to search in planet names."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum number of rows."),
) -> None:
    """Search the NASA Exoplanet Archive and print matching planets in a table."""
    from exoplanet_platform.ingestion.nasa_exoplanet_archive import (
        NASAExoplanetArchiveClient,
    )

    try:
        client = NASAExoplanetArchiveClient()
        with console.status(f"[cyan]Searching NASA EA for '{name}'..."):
            planets = client.search_planets(name=name, limit=limit)
    except ExoplanetPlatformError as e:
        _die(f"Search failed: {e}", e)
        return

    if not planets:
        console.print(f"[yellow]No planets matched '{name}'.[/yellow]")
        return
    console.print(_planet_table(planets, title=f"NASA EA results for '{name}'"))


@app.command("get")
def get_cmd(
    identifier: str = typer.Argument(..., help="Exact planet identifier, e.g. 'Kepler-10 b'."),
) -> None:
    """Fetch a single planet by exact identifier and print a detailed panel."""
    from exoplanet_platform.ingestion.nasa_exoplanet_archive import (
        NASAExoplanetArchiveClient,
    )

    try:
        client = NASAExoplanetArchiveClient()
        with console.status(f"[cyan]Fetching '{identifier}'..."):
            planet = client.get_planet(identifier)
    except ExoplanetPlatformError as e:
        _die(f"Fetch failed: {e}", e)
        return

    console.print(_planet_panel(planet))


@app.command("ingest")
def ingest_cmd(
    identifier: str = typer.Argument(..., help="Planet (or host star) identifier."),
    catalog: str = typer.Option(
        "nasa", "--catalog", "-c", help="Upstream catalog: 'nasa' or 'gaia'."
    ),
) -> None:
    """Fetch a record from an upstream catalog and persist it to the local DB."""
    from exoplanet_platform.storage import (
        PlanetRepository,
        StarRepository,
        get_session,
    )

    catalog_l = catalog.strip().lower()
    try:
        if catalog_l == "nasa":
            from exoplanet_platform.exceptions import (
                DataSourceNotFoundError,
                DataSourceUnavailableError,
            )
            from exoplanet_platform.ingestion.gaia import GaiaClient
            from exoplanet_platform.ingestion.nasa_exoplanet_archive import (
                NASAExoplanetArchiveClient,
            )

            nasa = NASAExoplanetArchiveClient()
            with console.status(f"[cyan]Fetching '{identifier}' from NASA EA..."):
                planet = nasa.get_planet(identifier)
            with get_session() as s:
                PlanetRepository(s).upsert(planet)
            console.print(
                f"[green]Stored planet[/green] [bold]{planet.identifier}[/bold] "
                f"(host star [bold]{planet.host_star}[/bold])."
            )

            # Auto-ingest host star via SIMBAD → Gaia so downstream habitability
            # / stellar-analysis commands work without a second manual step.
            with get_session() as s:
                star_exists = True
                try:
                    StarRepository(s).get(planet.host_star)
                except DataSourceNotFoundError:
                    star_exists = False

            if star_exists:
                console.print(
                    f"[dim]Host star[/dim] [bold]{planet.host_star}[/bold] "
                    "[dim]already stored, skipping Gaia lookup.[/dim]"
                )
            else:
                try:
                    gaia = GaiaClient()
                    with console.status(
                        f"[cyan]Resolving host star '{planet.host_star}' "
                        "via SIMBAD → Gaia..."
                    ):
                        star = gaia.resolve_by_name(planet.host_star)
                    with get_session() as s:
                        StarRepository(s).upsert(star)
                    console.print(
                        f"[green]Stored host star[/green] "
                        f"[bold]{star.identifier}[/bold] "
                        f"(Teff={star.effective_temperature_k} K, "
                        f"R={star.radius_solar} R☉)."
                    )
                except (DataSourceNotFoundError, DataSourceUnavailableError) as se:
                    console.print(
                        f"[yellow]Could not auto-ingest host star "
                        f"'{planet.host_star}':[/yellow] {se}"
                    )
                    console.print(
                        "[yellow]Run[/yellow] "
                        f"[cyan]exoplanet ingest '{planet.host_star}' "
                        "--catalog gaia[/cyan] "
                        "[yellow]to add it manually later.[/yellow]"
                    )
        elif catalog_l == "gaia":
            from exoplanet_platform.ingestion.gaia import GaiaClient

            client = GaiaClient()
            with console.status(f"[cyan]Fetching '{identifier}' from Gaia..."):
                star = client.get_star(identifier)
            with get_session() as s:
                repo = StarRepository(s)
                repo.upsert(star)
            console.print(
                f"[green]Stored star[/green] [bold]{star.identifier}[/bold]."
            )
        else:
            _die(f"Unsupported catalog '{catalog}'. Choose 'nasa' or 'gaia'.")
    except ExoplanetPlatformError as e:
        _die(f"Ingest failed: {e}", e)


@app.command("list-planets")
def list_planets_cmd(
    host_star: str | None = typer.Option(
        None, "--host-star", help="Filter by host star identifier."
    ),
    catalog: str | None = typer.Option(
        None, "--catalog", "-c", help="Filter by catalog (e.g. nasa_exoplanet_archive)."
    ),
    limit: int = typer.Option(100, "--limit", "-n", help="Maximum rows."),
) -> None:
    """List planets currently stored in the local database."""
    from exoplanet_platform.storage import PlanetRepository, get_session

    cat_enum: Catalog | None = None
    if catalog:
        try:
            cat_enum = Catalog(catalog)
        except ValueError:
            _die(
                f"Unknown catalog '{catalog}'. "
                f"Valid values: {', '.join(c.value for c in Catalog)}."
            )
            return
    try:
        with get_session() as s:
            repo = PlanetRepository(s)
            planets = repo.list(host_star=host_star, catalog=cat_enum, limit=limit)
    except ExoplanetPlatformError as e:
        _die(f"Database query failed: {e}", e)
        return

    if not planets:
        console.print(
            "[yellow]No planets stored yet. Try[/yellow] "
            "[cyan]exoplanet db init[/cyan] [yellow]then[/yellow] "
            "[cyan]exoplanet ingest <identifier>[/cyan]."
        )
        return
    console.print(_planet_table(planets, title="Planets in local DB"))


# --------------------------------------------------------------------------- #
# Orbit / Horizons
# --------------------------------------------------------------------------- #


@app.command("orbit")
def orbit_cmd(
    body: str = typer.Argument(..., help="Target body (e.g. 'Mars', '499')."),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)."),
    stop: str = typer.Option(..., "--stop", help="Stop date (YYYY-MM-DD)."),
    step: str = typer.Option("1d", "--step", help="Step size (e.g. '1d', '6h')."),
) -> None:
    """Fetch ephemeris from JPL Horizons; print first 10 rows and save full CSV."""
    from exoplanet_platform.ingestion.jpl_horizons import JPLHorizonsClient

    try:
        start_dt = datetime.fromisoformat(start)
        stop_dt = datetime.fromisoformat(stop)
    except ValueError as e:
        _die(f"Invalid date format: {e}", e)
        return

    try:
        client = JPLHorizonsClient()
        with console.status(f"[cyan]Querying Horizons for {body}..."):
            ephemeris = client.get_ephemeris(
                body,
                start_dt.date().isoformat(),
                stop_dt.date().isoformat(),
                step=step,
            )
    except ExoplanetPlatformError as e:
        _die(f"Horizons query failed: {e}", e)
        return

    # Horizons client returns a pandas DataFrame — normalize to list[dict].
    try:
        import pandas as pd

        if isinstance(ephemeris, pd.DataFrame):
            rows: list[dict] = ephemeris.to_dict(orient="records")
        else:
            rows = list(ephemeris)
    except ImportError:
        rows = list(ephemeris)
    if not rows:
        console.print(f"[yellow]No ephemeris rows returned for '{body}'.[/yellow]")
        return

    # Compact preview table (first 10 rows).
    preview = rows[:10]
    cols = list(preview[0].keys())
    table = Table(title=f"Ephemeris for {body} (first 10 rows)", header_style="bold cyan")
    for c in cols:
        table.add_column(c)
    for row in preview:
        table.add_row(*[str(row.get(c, "")) for c in cols])
    console.print(table)

    # Write full CSV.
    settings = get_settings()
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    safe_name = body.replace("/", "_").replace(" ", "_")
    out_path = data_dir / f"{safe_name}_ephemeris.csv"
    try:
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    except OSError as e:
        _die(f"Failed to write CSV {out_path}: {e}", e)
        return

    console.print(
        f"[green]Wrote {len(rows)} rows to[/green] [bold]{out_path}[/bold]."
    )


# --------------------------------------------------------------------------- #
# Light curve group
# --------------------------------------------------------------------------- #


@light_curve.command("download")
def light_curve_download_cmd(
    target: str = typer.Argument(..., help="Target star identifier (e.g. 'TIC 307210830')."),
    mission: str = typer.Option(
        "TESS", "--mission", help="Mission: TESS, Kepler, or K2."
    ),
    sector: int | None = typer.Option(
        None, "--sector", help="TESS sector number (optional)."
    ),
    quarter: int | None = typer.Option(
        None, "--quarter", help="Kepler quarter number (optional)."
    ),
) -> None:
    """Download a light curve for ``target``, store it in the DB and print a summary."""
    from exoplanet_platform.ingestion.mast import MASTClient
    from exoplanet_platform.storage import LightCurveRepository, get_session

    try:
        mission_enum = Catalog(mission.lower())
    except ValueError:
        _die(
            f"Unknown mission '{mission}'. Valid: TESS, Kepler, K2."
        )
        return

    try:
        client = MASTClient()
        with Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            task_id = progress.add_task(
                f"Downloading {mission} light curve for {target}", total=None
            )
            lc = client.download_light_curve(
                target, mission=mission_enum, sector=sector, quarter=quarter
            )
            progress.update(task_id, completed=1)

        with get_session() as s:
            repo = LightCurveRepository(s)
            stored_id = repo.save(lc)
    except ExoplanetPlatformError as e:
        _die(f"Light curve download failed: {e}", e)
        return

    n = len(lc.time_bjd)
    summary = Panel(
        "\n".join(
            [
                f"[bold cyan]DB id[/bold cyan]:           {stored_id}",
                f"[bold cyan]Target[/bold cyan]:          {lc.target}",
                f"[bold cyan]Mission[/bold cyan]:         {lc.mission.value}",
                f"[bold cyan]Sector/quarter[/bold cyan]:  "
                f"{lc.sector if lc.sector is not None else lc.quarter}",
                f"[bold cyan]Cadence[/bold cyan]:         "
                f"{lc.cadence_minutes} min",
                f"[bold cyan]Points[/bold cyan]:          {n}",
                f"[bold cyan]Time span[/bold cyan]:       "
                f"{(max(lc.time_bjd) - min(lc.time_bjd)):.3f} d" if n else "n/a",
            ]
        ),
        title="Light curve stored",
        border_style="green",
    )
    console.print(summary)


# --------------------------------------------------------------------------- #
# Analyze group
# --------------------------------------------------------------------------- #


@analyze.command("transit")
def analyze_transit_cmd(
    target: str = typer.Argument(..., help="Target star (used to fetch the light curve)."),
    mission: str = typer.Option("TESS", "--mission", help="Mission for MAST query."),
    min_period: float = typer.Option(0.5, "--min-period", help="Min period (days)."),
    max_period: float = typer.Option(50.0, "--max-period", help="Max period (days)."),
    sector: int | None = typer.Option(None, "--sector", help="TESS sector number."),
    quarter: int | None = typer.Option(None, "--quarter", help="Kepler quarter number."),
) -> None:
    """Download a light curve, run BLS, and print the top signals as a table."""
    from exoplanet_platform.analysis.transit import TransitDetector
    from exoplanet_platform.ingestion.mast import MASTClient

    try:
        mission_enum = Catalog(mission.lower())
    except ValueError:
        _die(f"Unknown mission '{mission}'.")
        return

    try:
        client = MASTClient()
        with console.status(f"[cyan]Downloading {mission} light curve for {target}..."):
            lc = client.download_light_curve(
                target, mission=mission_enum, sector=sector, quarter=quarter
            )

        detector = TransitDetector()
        with console.status("[cyan]Detrending..."):
            detrended = detector.detrend(lc)
        with console.status("[cyan]Running BLS search..."):
            signals = detector.search(
                detrended,
                min_period_days=min_period,
                max_period_days=max_period,
            )
    except ExoplanetPlatformError as e:
        _die(f"Transit analysis failed: {e}", e)
        return

    if not signals:
        console.print("[yellow]No transit signals found.[/yellow]")
        return

    table = Table(
        title=f"Top BLS signals for {target}",
        header_style="bold cyan",
    )
    table.add_column("#", justify="right")
    table.add_column("Period (d)", justify="right")
    table.add_column("Epoch (BJD)", justify="right")
    table.add_column("Duration (h)", justify="right")
    table.add_column("Depth (ppm)", justify="right")
    table.add_column("SNR", justify="right")
    table.add_column("Power", justify="right")
    for i, sig in enumerate(signals, 1):
        table.add_row(
            str(i),
            f"{sig.period_days:.6f}",
            f"{sig.epoch_bjd:.4f}",
            f"{sig.duration_hours:.3f}",
            f"{sig.depth_ppm:.1f}",
            f"{sig.snr:.2f}",
            f"{sig.power:.3f}",
        )
    console.print(table)


@analyze.command("habitability")
def analyze_habitability_cmd(
    planet_identifier: str = typer.Argument(
        ..., help="Planet identifier (must already be ingested)."
    ),
) -> None:
    """Assess habitable-zone membership for a stored planet and print the result."""
    from exoplanet_platform.analysis.habitability import HabitabilityAnalyzer
    from exoplanet_platform.storage import (
        PlanetRepository,
        StarRepository,
        get_session,
    )

    try:
        with get_session() as s:
            planet = PlanetRepository(s).get(planet_identifier)
            if planet is None:
                _die(
                    f"Planet '{planet_identifier}' not found in DB. "
                    f"Run `exoplanet ingest {planet_identifier}` first."
                )
                return
            star = StarRepository(s).get(planet.host_star)
        if star is None:
            _die(
                f"Host star '{planet.host_star}' not in DB. "
                f"Run `exoplanet ingest {planet.host_star} --catalog gaia`."
            )
            return

        analyzer = HabitabilityAnalyzer()
        assessment = analyzer.assess(planet, star)
    except ExoplanetPlatformError as e:
        _die(f"Habitability analysis failed: {e}", e)
        return

    cons_marker = (
        "[green]YES[/green]" if assessment.in_conservative_hz else "[red]NO[/red]"
    )
    opt_marker = (
        "[green]YES[/green]" if assessment.in_optimistic_hz else "[red]NO[/red]"
    )
    esi_str = (
        f"{assessment.earth_similarity_index:.3f}"
        if assessment.earth_similarity_index is not None
        else "n/a"
    )
    body = "\n".join(
        [
            f"[bold cyan]Planet[/bold cyan]:                  {assessment.planet}",
            f"[bold cyan]Host star[/bold cyan]:               {star.identifier}",
            "",
            f"[bold]Conservative HZ[/bold]:         {cons_marker}",
            f"[bold]Optimistic HZ[/bold]:           {opt_marker}",
            f"[bold]HZ inner edge[/bold]:           {assessment.hz_inner_au:.4f} AU",
            f"[bold]HZ outer edge[/bold]:           {assessment.hz_outer_au:.4f} AU",
            f"[bold]Earth Similarity Index[/bold]:  {esi_str}",
            f"[bold]Equilibrium T[/bold]:           "
            f"{assessment.equilibrium_temperature_k} K",
            "",
            f"[dim]{assessment.notes or ''}[/dim]",
        ]
    )
    console.print(
        Panel(
            body,
            title=f"Habitability: {assessment.planet}",
            border_style="green" if assessment.in_optimistic_hz else "yellow",
        )
    )


# --------------------------------------------------------------------------- #
# DB group
# --------------------------------------------------------------------------- #


@db.command("init")
def db_init_cmd() -> None:
    """Create all database tables (idempotent)."""
    from exoplanet_platform.storage import init_db

    try:
        with console.status("[cyan]Initialising database..."):
            init_db()
    except ExoplanetPlatformError as e:
        _die(f"DB init failed: {e}", e)
        return
    console.print("[green]Database initialised.[/green]")


# --------------------------------------------------------------------------- #
# Serve group
# --------------------------------------------------------------------------- #


@serve.command("api")
def serve_api_cmd(
    host: str | None = typer.Option(None, "--host", help="Bind host."),
    port: int | None = typer.Option(None, "--port", help="Bind port."),
    reload: bool = typer.Option(False, "--reload", help="Enable uvicorn autoreload."),
) -> None:
    """Run the FastAPI server via uvicorn programmatically."""
    import uvicorn

    settings = get_settings()
    bind_host = host or settings.api.host
    bind_port = port or settings.api.port
    console.print(
        f"[cyan]Starting API on[/cyan] [bold]http://{bind_host}:{bind_port}[/bold]"
    )
    try:
        uvicorn.run(
            "exoplanet_platform.api.main:app",
            host=bind_host,
            port=bind_port,
            reload=reload,
        )
    except Exception as e:
        _die(f"API server crashed: {e}", e)


@serve.command("dashboard")
def serve_dashboard_cmd(
    port: int = typer.Option(8501, "--port", help="Streamlit server port."),
) -> None:
    """Launch the Streamlit dashboard as a subprocess."""
    app_path = Path(__file__).resolve().parent.parent / "dashboard" / "app.py"
    if not app_path.exists():
        _die(f"Dashboard entry-point not found at {app_path}.")
        return
    console.print(
        f"[cyan]Launching dashboard on[/cyan] [bold]http://localhost:{port}[/bold]"
    )
    try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(app_path),
                "--server.port",
                str(port),
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        _die(f"Streamlit exited with code {e.returncode}.", e)
    except FileNotFoundError as e:
        _die("Streamlit is not installed in this environment.", e)


if __name__ == "__main__":  # pragma: no cover
    app()
