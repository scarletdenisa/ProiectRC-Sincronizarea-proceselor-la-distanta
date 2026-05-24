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

---

## Testare functionalitatii noi: semafoare cu coada de asteptare

### Testul 1 — coada de asteptare si notificare push

Deschide *3 terminale*.

*Terminal 1 - server:*
docker compose up --build


*Terminal 2 - clientA:*
python3 client/client.py --host localhost --port 6000 --name clientA

Scrie comanda:acquire sem1
Rezultat asteptat: OK: { "resource": "sem1", "owner": "clientA" }


*Terminal 3 — clientB* (in paralel cu clientA): python3 client/client.py --host localhost --port 6000 --name clientB

Scrie comanda (in timp ce clientA detine semaforul): acquire sem1

Rezultat asteptat - clientB nu primeste eroare, ci este pus in coada:
IN ASTEPTARE: semaforul 'sem1' este detinut de 'clientA'. Pozitia in coada: 1. Veti fi notificat cand primiti accesul.


*Inapoi in Terminal 2 — clientA elibereaza semaforul:* release sem1

Rezultat asteptat in *Terminal 3* :

[NOTIFICARE] Acces exclusiv acordat pentru semaforul 'sem1'! Puteti continua.


---

### Testul 2 — eliberare automata la deconectare

*Terminal 2 — clientA* dobandeste semaforul: acquire sem1


*Terminal 3 — clientB* intra in coada: acquire sem1


*Terminal 2* - inchidere clientA fortat cu Ctrl+C (sau scrie exit).

Rezultat asteptat in *Terminal 3* (apare automat):
[NOTIFICARE] Acces exclusiv acordat pentru semaforul 'sem1'! Puteti continua.

Serverul a detectat deconectarea lui clientA, a eliberat semaforul si l-a acordat lui clientB.

---

### Testul 3 — verificare stare server cu coada vizibila

Cu clientA detinand sem1 si clientB in coada, scrie in orice terminal conectat: status

Rezultat asteptat:
json
{
  "locks": { "sem1": "clientA" },
  "waiting_queues": { "sem1": ["clientB"] },
  "barriers": {},
  "clients": { ... }
}