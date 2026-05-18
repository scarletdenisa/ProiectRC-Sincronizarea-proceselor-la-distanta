# Sincronizarea proceselor la distanta
Aplicatie distribuita client-server pentru sincronizarea proceselor la distanta.

## Ce face proiectul
- Server TCP care accepta conexiuni de la mai multi clienti simultan.
- Protocol clar bazat pe JSON newline-delimitat.
- Client interactiv pentru trimiterea cererilor si primirea raspunsurilor.
- Sincronizare prin:
  - `acquire` / `release` pentru blocarea resurselor (mutual exclusion)
  - `barrier` pentru sincronizare la un punct de intalnire intre mai multi clienti
- Tratarea erorilor la date invalide, conexiune inchisa si timeouts.

## Structura proiectului
- `/server` - codul serverului si Dockerfile
- `/client` - codul clientului
- `docker-compose.yml` - configurarea pentru pornirea serverului in container
- `README.md` - instructiuni de utilizare

## Cerinte
- Docker si Docker Compose instalate
- Python 3.10+ pentru rularea clientului local

## Rulare server cu Docker
Din directorul radacina al proiectului se deschide un terminal si se scrie:

```bash
docker compose up --build
```

Serverul va fi disponibil la `localhost:6000`.

## Rulare client local
Se deschide un terminal nou si se scrie:

```bash
python3 client/client.py --host localhost --port 6000 --name clientA
```

## Comenzi client
- `acquire <name>` - cere blocarea resursei `<name>`
- `release <name>` - elibereaza resursa `<name>`
- `barrier <name> <participants>` - se sincronizeaza cu ceilalti clienti la bariera `<name>`
- `status` - cere starea curenta a serverului
- `ping` - verifica ca serverul raspunde
- `exit` - inchide clientul

## Scenariu demo
1. Se porneste serverul cu `docker compose up --build`
2. Se deschid doua sau trei terminale diferite
3. Se ruleaza `python3 client/client.py --host localhost --port 6000 --name client1`, `client2`, `client3`
4. Pe fiecare client se executa:
   - `barrier start 3` si se asteapta ca toti cei 3 clienti sa ajunga la bariera
   - `acquire resource1` de la unul dintre clienti si `release resource1`
5. Trebuie verificat ca serverul raspunde si ca erorile sunt tratate corect

