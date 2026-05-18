# Sincronizarea proceselor la distanță

Acest proiect implementează o aplicație distribuită client-server pentru sincronizarea proceselor la distanță.

## Ce face proiectul
- Server TCP care acceptă conexiuni de la mai mulți clienți simultan.
- Protocol clar bazat pe JSON newline-delimitat.
- Client interactiv pentru trimiterea cererilor și primirea răspunsurilor.
- Sincronizare prin:
  - `acquire` / `release` pentru blocarea resurselor (mutual exclusion)
  - `barrier` pentru sincronizare la un punct de întâlnire între mai mulți clienți
- Tratarea erorilor la date invalide, conexiune închisă și timeouts.

## Structura proiectului
- `/server` - codul serverului și Dockerfile
- `/client` - codul clientului
- `docker-compose.yml` - configurarea pentru pornirea serverului în container
- `README.md` - instrucțiuni de utilizare

## Cerințe
- Docker și Docker Compose instalate
- Python 3.10+ pentru rularea clientului local

## Rulare server cu Docker
Din directorul rădăcină al proiectului:

```bash
docker compose up --build
```

Serverul va fi disponibil la `localhost:6000`.

## Rulare client local
Deschide un terminal nou și rulează:

```bash
python3 client/client.py --host localhost --port 6000 --name clientA
```

## Comenzi client
- `acquire <name>` - cere blocarea resursei `<name>`
- `release <name>` - eliberează resursa `<name>`
- `barrier <name> <participants>` - se sincronizează cu ceilalți clienți la bariera `<name>`
- `status` - cere starea curentă a serverului
- `ping` - verifică că serverul răspunde
- `exit` - închide clientul

## Scenariu demo
1. Pornești serverul cu `docker compose up --build`
2. Deschizi două sau trei terminale diferite
3. Rulezi `python3 client/client.py --host localhost --port 6000 --name client1`, `client2`, `client3`
4. Pe fiecare client execuți:
   - `barrier start 3` și aștepți ca toți cei 3 clienți să ajungă la barieră
   - `acquire resource1` de la unul dintre clienți și `release resource1`
5. Verifici că serverul răspunde și că erorile sunt tratate corect

## Link video demonstrativ
- 🟣 Video demo: https://example.com/demo-video

> NOTĂ: Înlocuiește linkul de mai sus cu URL-ul real al înregistrării demonstrative.
