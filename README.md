# ORT runner

Een Go-job die repositories ophaalt uit het OSS-register, ze achter elkaar onderzoekt
met [OSS Review Toolkit (ORT)](https://github.com/oss-review-toolkit/ort) en per
repository een resultaat naar een instelbaar POST-endpoint stuurt. De pipeline is
**Analyzer → Advisor (OSV) → Evaluator**. De regels komen uit de afzonderlijk
versieerbare [ort-config](https://github.com/developer-overheid-nl/ort-config).

De runner en ORT draaien samen in één container. De Go-code roept de bestaande
`ort`-commands rechtstreeks aan. Eén aanroep verwerkt één batch en stopt daarna;
een scheduler of applicatie kan de container starten. Voor losse tests kun je ook
één repository op een specifieke commit scannen.

## Repositories uit het register verwerken

Het bestaande endpoint is `GET /oss-register/v1/repositories`. De response is een
JSON-array met onder andere `id` en `url`. De runner haalt eerst alle pagina's op
met `page`, `perPage=100` en de `Total-Pages`-header. Filters in de opgegeven URL
blijven behouden. Een eventueel bestaand `publiccode`-filter wordt vervangen. De
runner haalt zowel de basis-URL als dezelfde URL met `publiccode=false` op en voegt
beide verzamelingen samen op repository-id. Overlap
wordt één keer gescand. Bij een scanfout gaat hij door met de volgende repository.

Het register levert geen commit-SHA. Bij iedere checkout haalt de runner de
defaultbranch op en legt de werkelijk gescande commit vast in het resultaat.
Repositories uit het register moeten een HTTP(S)-Git-URL hebben.

Benodigd: Docker en een configuratievolume. Vul dit volume vanuit een release van
`ort-config`; een lokale checkout is daardoor niet nodig. Stel voor de register-GET
`ORT_REGISTER_API_KEY` in. De OAuth-gegevens `AUTH_TOKEN_URL`,
`AUTH_CLIENT_ID`, `AUTH_CLIENT_SECRET` en `AUTH_SCOPES` zijn uitsluitend voor de
resultaat-POST.

```sh
docker build -t ort-runner:dev .
mkdir -p output
docker volume create ort-config
docker run --rm \
  -v ort-config:/target \
  ghcr.io/developer-overheid-nl/ort-config:v0.0.1

docker run --rm --init \
  --user "$(id -u):$(id -g)" \
  --env-file .env.local \
  -e HOME=/tmp \
  -v ort-config:/config:ro \
  -v "$PWD/output:/output" \
  ort-runner:dev \
  --repositories-url "https://api.developer.overheid.nl/oss-register/v1/repositories"
```

**Het POST-endpoint bestaat nog niet.** Laat `ORT_RESULTS_URL` voorlopig leeg:
dan bewaart de runner de te versturen berichten lokaal. Zodra het endpoint bestaat,
stel je `ORT_RESULTS_URL` in op het volledige adres, of gebruik je `--results-url`.
De OAuth-client verzorgt straks uitsluitend de authenticatie van de POST. De
bestaande `POST /repositories` registreert repositories en is niet het doel voor
scanresultaten.

URLs zijn ook in te stellen met `ORT_REPOSITORIES_URL` en `ORT_RESULTS_URL`; CLI-opties
gaan voor. `--http-timeout` geldt per GET/POST en is standaard `30s`. Voor een
resultaat-POST vraagt de runner zelf een token aan, bewaart dat tijdens de job en
vernieuwt het voor afloop. De gedeelde implementatie staat in
`don-register-common/auth`. Credentials worden niet aan Git of ORT doorgegeven.
`AUTH_TOKEN_URL`, `AUTH_CLIENT_ID` en `AUTH_CLIENT_SECRET` moeten samen ingevuld
zijn. `AUTH_SCOPES` is optioneel en mag leeg blijven; meerdere scopes worden met
spaties gescheiden.

### Voorstel voor het POST-bericht

Per repository wordt één JSON-bericht verstuurd, ook als de scan mislukt. Het bevat
`schemaVersion: 1`, het register-id in `repositoryId` en het volledige `run.json`-object
onder `scan`. Dit is het voorlopige contract voor het nog te bouwen endpoint.
Ingekort voorbeeld:

```json
{
  "schemaVersion": 1,
  "repositoryId": "id-uit-het-register",
  "scan": {
    "status": "completed",
    "repository": "https://github.com/example/project",
    "revision": "0123456789012345678901234567890123456789",
    "vulnerabilities": [
      {
        "package_id": "Go::example.org/module:1.0.0",
        "id": "GO-2026-1234",
        "summary": "Example vulnerability",
        "severity": "HIGH",
        "score": 7.5,
        "scoring_system": "CVSS3",
        "first_fixed_versions": ["1.1.0"]
      }
    ],
    "findings": [
      {"rule": "MISSING_SECURITY_FILE", "severity": "ERROR", "message": "Missing SECURITY.md"}
    ]
  }
}
```

### Batchresultaten

```text
batch-123456/
├── batch.json
└── repository-234567/
    ├── submission.json
    └── run-345678/
        ├── run.json
        ├── analyzer-result.yml
        ├── advisor-result.yml
        ├── evaluation-result.yml
        └── ...logs
```

`submission.json` bevat exact het verstuurde of nog te versturen bericht. In
`batch.json` staan alle verwerkte register-id's met hun scanstatus en afleverstatus:
`posted`, `failed`, `not_configured` of `pending` bij een afgebroken batch.

Een mislukte POST maakt de batch onvolledig, bewaart het bericht en stopt de overige
scans niet. Er zijn geen automatische POST-retries zolang het endpoint geen afspraken
over dubbele berichten heeft. Een volgende batch scant opnieuw; bewaarde berichten
worden niet automatisch opnieuw verstuurd. Een fout bij het ophalen van de lijst
stopt de batch voordat er scans starten.

## Eén repository testen

Benodigd: Docker, Git en een gevuld `ort-config`-volume. Onderstaand voorbeeld
bepaalt eerst de huidige commit van `don-crawler`; tijdens de scan blijft die commit
vaststaan.

```sh
docker build -t ort-runner:dev .
docker volume create ort-config
docker run --rm \
  -v ort-config:/target \
  ghcr.io/developer-overheid-nl/ort-config:v0.0.1

DON_ORT_REPOSITORY="https://github.com/developer-overheid-nl/don-crawler.git"
DON_ORT_REVISION="$(git ls-remote "$DON_ORT_REPOSITORY" HEAD | cut -f1)"
mkdir -p ../don-crawler-ort-output

docker run --rm --init \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -v ort-config:/config:ro \
  -v "$PWD/../don-crawler-ort-output:/output" \
  ort-runner:dev \
  --repository "$DON_ORT_REPOSITORY" \
  --revision "$DON_ORT_REVISION"
```

De UID/GID-optie maakt outputbestanden leesbaar voor de huidige gebruiker op Linux;
`HOME=/tmp` geeft de tools een schrijfbare home-directory. Voor een lokale Git-repository
kun je die extra read-only mounten en bijvoorbeeld `--repository file:///source`
meegeven. Deze mount moet gedurende alle drie de ORT-stappen beschikbaar blijven.

Gebruik `docker run --rm ort-runner:dev --help` voor alle opties. De belangrijkste:

| Optie | Betekenis | Standaard |
| --- | --- | --- |
| `--repositories-url` | GET-endpoint voor een batch | `ORT_REPOSITORIES_URL` |
| `--results-url` | POST-endpoint voor batchresultaten | `ORT_RESULTS_URL`, anders alleen lokaal bewaren |
| `--repository` | Git-URL of lokaal Git-pad voor een losse scan | verplicht bij losse scan |
| `--revision` | Volledige commit-SHA van 40 tekens | verplicht bij losse scan |
| `--config-dir` | ORT-configuratie | `/config` |
| `--output-dir` | Bovenliggende map voor scanresultaten | `/output` |
| `--stage-timeout` | Maximale duur per checkout of ORT-stap | `30m` |

De checkout bevat ook submodules op hun vastgelegde commits. Per scan maakt de runner
een tijdelijke kopie van de configuratie en legt daarvan een SHA-256-vingerafdruk vast.
Gebruik voor herhaalbare scans steeds dezelfde config-release of hetzelfde image-digest.
De configuratiemap hoeft alleen een regulier bestand `evaluator.rules.kts` te bevatten.

## Resultaten en exitcodes

Elke losse scan maakt een nieuwe `run-*`-map onder de outputmap. Binnen een batch
staat deze map onder de bijbehorende `repository-*`-map:

```text
run-123456/
├── run.json
├── checkout.log
├── analyze.log
├── advise.log
├── evaluate.log
├── analyzer-result.yml
├── advisor-result.yml
└── evaluation-result.yml
```

`run.json` bevat de commit, config-vingerafdruk, image, ORT-versies, tijden, status
en exitcode per stap, de OSV-kwetsbaarheden en de regelovertredingen. Per kwetsbaarheid
worden het package, advisory-id, samenvatting, beschikbare score en eerste opgeloste
versies opgenomen. Bij een fout blijven reeds gemaakte resultaten en logs bewaard.
De tijdelijke broncode en config-kopie worden opgeruimd.

| Runner-exitcode | Betekenis |
| --- | --- |
| `0` | Alle scans afgerond, eventueel met regelovertredingen; ingestelde POSTs geaccepteerd |
| `1` | Scan, batch of verzending onvolledig/mislukt; zie `batch.json`, `run.json` en logs |
| `2` | Verplichte CLI-optie ontbreekt, onbekende optie of conflicterende modi |

Zonder ingesteld POST-endpoint betekent exitcode `0` dat de resultaten lokaal zijn
bewaard. De afleverstatus blijft dan `not_configured`.

Evaluator-exitcode `2` met regelovertredingen is een afgeronde evaluatie: bijvoorbeeld
een ontbrekend `SECURITY.md` wordt een bevinding. Analyzer- of Advisor-problemen
maken de scan onvolledig. Als hun resultaat bruikbaar is, gaan volgende stappen
wel door, zodat ook de repositoryregels nog uitgevoerd kunnen worden. Een ontbrekend
of onleesbaar resultaat stopt de vervolgstappen.

`completed` beschrijft de uitvoering; het is geen verklaring dat een project veilig
of volledig onderzocht is. `package_count: 0` betekent dat geen dependencies zijn
gevonden. Sommige OSV-fouten in ORT 92.4.0 verschijnen alleen in logs; de runner kan
alleen problemen classificeren die ORT in exitcodes of resultaten teruggeeft.
De huidige pipeline bevat geen Scanner-stap en hydrateert Git LFS-bestanden niet.

## ORT-versies afzonderlijk testen

De Dockerfile gebruikt ORT `92.4.0`, vastgezet met een image-digest. Bouw een kandidaat
door alleen het buildargument te wijzigen:

```sh
docker build \
  --build-arg ORT_IMAGE=ghcr.io/oss-review-toolkit/ort:92.4.0 \
  --build-arg RUNNER_VERSION="$(git rev-parse HEAD)" \
  -t ort-runner:candidate .

ORT_RUNNER_TEST_IMAGE=ort-runner:candidate \
ORT_RUNNER_TEST_CONFIG_DIR="$PWD/../ort-config" \
  go test -tags=integration ./internal/runner -run TestContainerBaseline -count=1 -timeout=20m -v
```

Vervang de image door de gewenste tag of digest. De containertest gebruikt twee
kleine Git-fixtures: alle basisbestanden aanwezig, en één ontbrekend `SECURITY.md`.
Hij controleert de echte drie ORT-stappen, resultaten en regelovertredingen.
De fixtures hebben geen dependencies; ze testen geen live OSV-dekking of alle
package managers. Test daarvoor ook representatieve projectrepositories.

De GitHub-workflow voert Go-tests en deze containertest uit. Met **Run workflow**
kun je een kandidaat-image en config-revisie opgeven. Gewone pushes en pull requests
publiceren geen image.

## Releasen

Een semver-tag met de vorm `v*.*.*` start na de tests de publicatie van het
multi-platform runner-image en maakt een GitHub Release:

```text
ghcr.io/developer-overheid-nl/ort-runner:v0.0.1
ghcr.io/developer-overheid-nl/ort-runner:<commit-sha>
```

Maak een release vanaf de gewenste commit met:

```sh
git tag v0.0.1
git push origin v0.0.1
```

## Ontwikkelen

Gebruik de Go-versie uit `go.mod` en Git. Go voert de passende toolchain automatisch
uit wanneer toolchain-downloads zijn ingeschakeld.

```sh
go test -race ./...
go vet ./...
go build -o bin/ort-runner ./cmd/ort-runner
```

De Go-tests gebruiken echte tijdelijke Git-repositories, lokale HTTP-testservers en
een testprogramma voor de ORT-procesgrens. Ze testen paginering, POST-berichten,
doorlopen na fouten, exacte/defaultbranch-commits, submodules, time-outs, ontbrekende
output en het onderscheid tussen bevindingen en uitvoeringsproblemen.
Voor rechtstreeks lokaal uitvoeren op Linux/macOS moeten Git en ORT geïnstalleerd
zijn; via `--ort-binary` kun je een specifieke ORT-installatie aanwijzen.
