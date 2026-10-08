# Bijdragen

Bijdragen aan de ORT-runner zijn welkom: meldingen van fouten, ideeën en pull
requests.

## Een issue melden

Meld een fout of wens via een
[GitHub-issue](https://github.com/developer-overheid-nl/ort-runner/issues). Beschrijf
wat je verwachtte, wat er gebeurde en hoe het te reproduceren is. Meld een
beveiligingsprobleem niet via een issue; zie [SECURITY.md](SECURITY.md).

## Een wijziging voorstellen

1. Maak een branch vanaf `develop`.
2. Voeg tests toe voor nieuw of gewijzigd gedrag.
3. Controleer de wijziging:

   ```sh
   go test -race ./cmd/... ./internal/...
   go vet ./cmd/... ./internal/...
   ```

4. Beschrijf de wijziging voor de changelog met
   [Changie](https://github.com/miniscruff/changie): `changie new`.
5. Open een pull request naar `develop`.

Door bij te dragen ga je ermee akkoord dat je bijdrage onder de
[EUPL-1.2](LICENSE) valt. Houd je daarbij aan de [gedragscode](CODE_OF_CONDUCT.md).
