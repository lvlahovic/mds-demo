# Order API + Inventory Processing Service

Mali event-driven sistem od dva nezavisna Spring Boot servisa koji komuniciraju
preko Redis Streams-a:

- **order-service** izlaže `POST /orders` i objavljuje order event za svaki
  prihvaćen zahtev.
- **inventory-service** konzumira te evente, simulira rezervaciju zaliha
  (dovoljna količina na stanju → rezervisano, u suprotnom → odbijeno) i drži
  zalihe in-memory.
- **inventory-service** zatim objavljuje ishod nazad na drugom stream-u, koji
  **order-service** konzumira da bi narudžbinu preveo u stvarno terminalno
  stanje - tako da `GET /orders/{orderId}` odgovara šta se zapravo desilo, a
  `GET /orders/{orderId}/status` to strimuje uživo.

Opis zadatka je evaluaciju ograničio na **kvalitet i pouzdanost integracije**
između dva servisa, ne na poslovnu logiku samu po sebi - odluke u nastavku
proizilaze iz toga.

## Pokretanje

Potreban je Docker (Docker Desktop na Windows/Mac, ili Docker Engine +
`compose` plugin na Linux-u). Nije potreban lokalni JDK ni Maven - build se
dešava unutar Docker build faze.

```bash
docker compose up --build
```

Ovo podiže tri kontejnera:

| Servis               | Port  | Uloga                                          |
|-----------------------|-------|--------------------------------------------------|
| `redis`               | 6379  | Broker (Redis Streams), AOF perzistencija uključena |
| `order-service`       | 8080  | `POST /orders`, status narudžbine (uklj. SSE)     |
| `inventory-service`   | -     | Pozadinski konzument, bez HTTP interfejsa         |

Slanje zahteva (dostupno i kao
`order-service/src/main/java/com/lvl/mds/orderapi/request.http` za IntelliJ-ov
HTTP klijent):

```bash
curl -i -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"order-1","itemId":"item-1","quantity":2}'
```

`order-service` odgovara sa `202 Accepted` čim je event trajno objavljen -
to znači samo da je narudžbina prosleđena dalje, ne da su zalihe rezervisane.
Ishod rezervacije stiže asinhrono preko `order-results-stream`-a, pa se čita
sa same narudžbine:

```bash
curl -s http://localhost:8080/orders/order-1
```

```json
{"orderId":"order-1","itemId":"item-1","quantity":2,"status":"RESERVED",
 "statusReason":"reserved 2 of item 'item-1'","updatedAt":"2026-08-18T12:00:00.123Z"}
```

Ili se pretplatiti umesto anketiranja (polling) - stream odmah šalje trenutni
status, zatim svaku promenu, i zatvara se čim narudžbina uđe u terminalno
stanje:

```bash
curl -N http://localhost:8080/orders/order-1/status
```

```
event:status
data:{"orderId":"order-1", ... ,"status":"PUBLISHED", ...}

event:status
data:{"orderId":"order-1", ... ,"status":"RESERVED", ...}
```

Statusi narudžbine: `CREATED` (prihvaćena lokalno, još nije objavljena) →
`PUBLISHED` (na brokeru, čeka odluku) → `RESERVED`,
`REJECTED_INSUFFICIENT_STOCK`, `REJECTED_UNKNOWN_ITEM`, ili `FAILED`
(inventory-service je poslao u dead-letter bez ikad donete odluke).

Seed-ovane zalihe (`inventory-service`, in-memory, resetuju se pri restartu):

| itemId   | količina |
|----------|----------|
| item-1   | 100      |
| item-2   | 50       |
| item-3   | 5        |

## Arhitektonske odluke

### Redis Streams, ne Redis Pub/Sub

Redis Pub/Sub nema perzistenciju ni redelivery: ako je `inventory-service`
nedostupan (ili nasred restarta) u trenutku objave eventa, event je prosto
izgubljen. To ne zadovoljava zahtev "pouzdane integracije". Redis Streams
čuva svaki zapis (uz AOF, videti niže), podržava consumer group-e sa
potvrdom (ack) po poruci, i omogućava da poruka koja nikad nije potvrđena
bude reklamovana i ponovo obrađena. Ovo je i pattern koji koristim za
ekvivalentan problem na poslu, pa je ujedno i onaj koji mogu najdublje da
objasnim.

### Mehanizam pouzdanosti

- **At-least-once isporuka, eksplicitan ack.** `inventory-service` čita preko
  `XREADGROUP` na consumer group-i `inventory-service-group` i tek nakon što
  je poruka u potpunosti obrađena radi `XACK`. Ako obrada baci izuzetak,
  poruka namerno ostaje nepotvrđena - ostaje u Pending Entries List-i (PEL)
  te grupe umesto da bude izgubljena.
- **Idempotencija.** Pošto redelivery unutar consumer group-e (ili ponovni
  pokušaj producenta) može istom `orderId`-ju dvaput proslediti
  `inventory-service`-u, `ProcessedOrdersStore` čuva in-memory
  `Map<orderId, ishod>` već obrađenih narudžbina. Duplirana isporuka ne
  rezerviše zalihe po drugi put - ali ponovo objavljuje zapamćeni ishod, jer
  do redelivery-ja dolazi baš onda kad prvi pokušaj nije čisto završen, što
  je ujedno slučaj u kom `order-service` možda nikad nije ni primio rezultat.
- **Retry / dead-letter queue.** `PendingMessagesReclaimer` je `@Scheduled`
  posao koji proverava `XPENDING` na svakih
  `inventory.retry.scan-interval-ms` (podrazumevano 10s), tražeći stavke
  neaktivne duže od `inventory.retry.pending-threshold-ms` (podrazumevano
  30s) - npr. zato što je konzument pao usred obrade. Radi `XCLAIM` nad tim
  stavkama i ponovo ih obrađuje kroz isti put obrade kao i živi konzument.
  Kada broj isporuka poruke pređe `inventory.retry.max-attempts`
  (podrazumevano 3), poruka se upisuje u `orders-stream-dlq` i potvrđuje na
  originalnom stream-u, tako da poruka koja trajno ne uspeva ne može da se
  vrti unedogled. Takođe emituje `FAILED` ishod, jer je i odustajanje ipak
  odgovor koji `order-service` čeka.
- **AOF perzistencija.** `redis-server --appendonly yes`, tako da
  `orders-stream` (i pozicija/PEL consumer group-e) prežive restart Redis
  kontejnera.
- **`redis:8-alpine`.** Redis je od verzije 7.4 do 7.x bio dvostruko
  licenciran pod RSALv2/SSPLv1 (nije OSI-odobrena open-source licenca).
  Redis 8 (maj 2025) je vratio AGPLv3 opciju, koja jeste OSI-odobrena - zato
  compose fajl konkretno fiksira `redis:8-alpine`, da bi zadovoljio zahtev
  zadatka za "open-source broker", ne samo "bilo koji Redis image".

### Ishod rezervacije se vraća preko drugog stream-a

`inventory-service` objavljuje svaku odluku na `order-results-stream`, a
`order-service` je konzumira preko sopstvene consumer group-e
`order-service-group` - ista mehanika kao odlazna grana, samo u ogledalu.
Bez toga bi `order-service` mogao da tvrdi samo "predato brokeru", a stvarni
ishod rezervacije ne bi živeo nigde osim u logu konzumenta.

Razmatrane su i odbačene dve alternative:

- **HTTP callback od `inventory-service`-a ka `order-service`-u.** Ovo ponovo
  uvodi baš onu sinhronu spregu koju broker treba da ukloni:
  `inventory-service` bi morao da zna gde `order-service` živi, bio bi
  blokiran njegovom nedostupnošću, i morao bi da razvije sopstvenu
  retry/timeout mašineriju pored one koju Redis Streams već pruža.
- **Deljeni Redis ključ koji `order-service` anketira (poll).** Nema
  redosleda, nema redelivery-ja, nema backlog-a dok je čitalac nedostupan -
  odbacuje baš ono što je Streams učinilo ispravnim izborom.

Redosled unutar `inventory-service`-a je namerno ovakav: rezerviši → zapamti
ishod lokalno → `XADD` rezultat → `XACK` narudžbinu. Ako `XADD` rezultata
ne uspe, order event ostaje u PEL-u, a redelivery pronalazi zapamćeni ishod
i ponovo objavljuje rezultat umesto da rezerviše po drugi put.

`order-service` tretira rezultat za nepoznatu narudžbinu, kao i drugi
rezultat za narudžbinu koja već ima odgovor, kao uobičajene no-op slučajeve,
a ne greške - pod at-least-once isporukom oba su normalan saobraćaj. Njegov
start takođe drenira sopstvene pending stavke, jer živi listener čita samo
poruke koje nikad nisu isporučene, a sve što je ostalo nepotvrđeno usled
pada bi inače zauvek ostalo u PEL-u.

### `GET /orders/{orderId}/status` kao Server-Sent Events

Pošto ishod stiže asinhrono, alternativa za klijenta je anketiranje
(`GET /orders/{orderId}`) dok status ne prestane da bude `PUBLISHED`. SSE
endpoint odmah šalje trenutni status, zatim svaku promenu, pa zatvara
konekciju čim narudžbina dostigne terminalno stanje - ograničena pretplata,
ne otvoreni kanal bez kraja.

SSE umesto WebSocket-a jer je saobraćaj strogo jednosmeran, radi se o
običnom HTTP-u (bez protokol upgrade-a, bez dodatne zavisnosti), a
`EventSource` se sam rekonektuje. Slanje snapshot-a pre praćenja promena je
ono što čini endpoint bezbednim za poziv u bilo kom trenutku, uključujući i
nakon što je rezultat već stigao.

Interno, messaging sloj ne zna da endpoint postoji: `OrderService` objavljuje
in-JVM `OrderStatusChangedEvent`, a `OrderStatusStream` u `web` paketu se na
njega pretplaćuje. Time zavisnost ostaje usmerena od web ka services sloju,
ne obrnuto.

### Graceful shutdown

`docker compose stop` (kao i Kubernetes-ovo gašenje pod-a) šalje `SIGTERM`,
pa zatim čeka `stop_grace_period` pre nego što eskalira na `SIGKILL`. Bez
dodatne intervencije, Spring-ova podrazumevana reakcija na `SIGTERM` je
bliža "prestani da primaš novi posao i odbaci sve što je u toku" nego
stvarnom draniranju - tri odvojene stvari su morale da se reše da bi se
zaista završilo ono što je bilo u toku:

- **Stream listeneri.** `StreamMessageListenerContainer.stop()` sam po sebi
  samo obori flag i vrati se odmah; poll thread može i dalje biti unutar
  `onMessage()`-a. Pošto se Redis connection factory gasi kasnije od skoro
  svega ostalog (`SmartLifecycle` faza `0`), trka do zatvaranja konteksta bi
  značila da završni `XACK` može pogoditi konekciju koja je već zatvorena -
  rezervisano, objavljeno, ali ipak ponovo isporučeno. `StreamListenerLifecycle`
  (postoji u oba servisa, po jedan po listeneru) obavija kontejner kao
  `SmartLifecycle` na fazi `Integer.MAX_VALUE` (gasi se prvi) i blokira u
  `stop()`-u dok pretplata ne prijavi da je stvarno izašla iz event loop-a,
  ograničeno sa `{service}.shutdown.listener-drain-timeout-ms` da zaglavljeni
  konzument ne bi mogao zauvek da drži shutdown otvorenim.
- **Otvorene SSE konekcije.** Uz `server.shutdown=graceful`, Tomcat čeka da
  se svaki aktivan zahtev završi pre nego što se ugasi - a SSE pretplata je,
  po dizajnu, aktivan zahtev koji se ne završava dok narudžbina ne bude
  odlučena. Jedan klijent koji gleda narudžbinu na čekanju bi zaustavio svaki
  shutdown do isteka timeout-a. `OrderStatusStream` je takođe `SmartLifecycle`,
  jednu fazu iznad Boot-ove `WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE`:
  završava svaki otvoreni emitter (čist kraj stream-a, ne reset) pre nego što
  Tomcat počne da čeka bilo šta, a pokušaj pretplate koji stigne nakon te
  tačke dobija trenutni snapshot i odmah zatvaranje, umesto konekcije koja bi
  jednostavno nadživela proces.
- **Reclaim posao.** `PendingMessagesReclaimer` radi `XCLAIM` → obradu →
  `XACK` kao jednu celinu; prekid njegove niti nasred prolaza bi ostavio
  poruku "zaklajmovanu" od konzumenta koji više ne postoji, čekajući istek
  praga ponovo, uzalud. `spring.task.scheduling.shutdown.await-termination=true`
  dozvoljava da se prolaz koji je u toku završi umesto da bude prekinut.

`spring.lifecycle.timeout-per-shutdown-phase` (20s) je poslednja brana iznad
svega ovoga, a `stop_grace_period: 30s` u compose fajlu je postavljen iznad
te vrednosti - kraći grace period bi doveo do `SIGKILL`-a nasred drenaže i
učinio ceo mehanizam besmislenim. Verifikovano uživo: gašenje
`inventory-service`-a preko `redis-cli` ne pokazuje nijednu preostalu poruku
u PEL-u, a log ide pravo sa "Stopping stream listener" na "drained" za oko
1s; gašenje `order-service`-a dok SSE klijent drži pretplatu na narudžbinu
koja je još `PUBLISHED` pokazuje redosled: drenaža listenera, zatim "Closed
1 open status subscription(s)", pa Tomcat-ov sopstveni graceful shutdown
koji ne nalazi ništa više da čeka - `curl -v` na klijentskoj strani
potvrđuje čist kraj chunked odgovora (`Connection ... left intact`, bez
reset-a), a ceo shutdown je trajao oko sekundu naspram 30-sekundnog grace
perioda.

### Sibling folderi, ne Maven multi-module build

`order-service` i `inventory-service` su dva nezavisna Maven projekta unutar
jednog Git repozitorijuma, svaki sa svojim `pom.xml`-om, Maven wrapper-om i
`Dockerfile`-om - ne multi-module build sa zajedničkim parent `pom.xml`-om.
Opis zadatka ova dva servisa opisuje kao nezavisne, koji komuniciraju preko
brokera; zajednički parent/reactor build bi uveo build-time spregu koja to
ne odražava (a ni stvaran deployment je ne bi imao - svaki servis se
isporučuje i verzioniše samostalno). Dupliranje nekoliko linija `pom.xml`-a
je mala cena za očuvanje te granice.

### Struktura paketa

Svaki servis prati istu horizontalnu/slojevitu konvenciju: `model` (domenski
tipovi), `dto` (request/response payload-i), `repository` (perzistencija -
ovde in-memory), `services` (poslovna logika), `web` (kontroleri), `config`
(wiring/bean-ovi), i poseban `messaging` paket posvećen Redis Streams
integraciji - odvojen od `config`-a jer je najspecifičniji za izbor brokera
u ovom zadatku.

## Ručno testirani scenariji

Ovi scenariji su ručno provereni protiv `docker compose up --build`:

1. **Happy path** - `POST /orders` sa `quantity` unutar raspoložive zalihe →
   `202 Accepted`, `inventory-service` loguje `RESERVED`, raspoloživa
   količina za taj artikal se smanjuje, a narudžbina prelazi u `RESERVED`
   na `GET /orders/{orderId}` i na otvorenom SSE stream-u.
2. **Nedovoljna zaliha** - zahtev za količinom iznad seed-ovane (npr.
   `item-3`, seed-ovan na 5, zatražen sa 999) → event se ipak objavljuje i
   vraća se `202 Accepted` (objava znači samo "prihvaćeno za obradu"),
   `inventory-service` loguje odbijanje zbog nedovoljne zalihe; ništa se ne
   oduzima, a narudžbina završava kao `REJECTED_INSUFFICIENT_STOCK` sa
   popunjenim razlogom.
3. **Pad konzumenta usred obrade** - zaustaviti `inventory-service`
   (`docker compose stop inventory-service`) dok je poruka u obradi ili
   nepotvrđena, pa ga ponovo pokrenuti
   (`docker compose start inventory-service`). Poruka je i dalje u PEL-u;
   `PendingMessagesReclaimer` je preuzima i ponovo obrađuje čim protekne
   prag neaktivnosti, bez potrebe da `order-service` bilo šta ponovo
   objavi.
4. **Duplirana isporuka** - isti `orderId` stigne dvaput (simulirano ručnim
   ponovnim objavljivanjem preko `redis-cli XADD`) rezerviše se samo
   jednom; druga isporuka se loguje kao duplikat i ipak potvrđuje, a
   zapamćeni ishod se ponovo objavljuje kako se ne bi izgubio zajedno s
   njom.
5. **Live status stream** - `curl -N http://localhost:8080/orders/order-1/status`
   otvoren odmah nakon `POST /orders` prima `PUBLISHED` odmah, zatim
   terminalni status čim `inventory-service` javi rezultat, pa se konekcija
   zatvara. Otvaranje naknadno daje jedan event i zatvara se.
6. **Graceful shutdown** - zaustaviti `inventory-service` sa porukom koju
   još nije potvrdio: log ide pravo sa "Stopping stream listener" na
   "drained", `XPENDING` na `orders-stream` je prazan, a kontejner se gasi
   za oko sekundu, daleko unutar 30s grace perioda. Zaustaviti
   `order-service` dok je `curl -N .../status` prikačen na narudžbinu koja
   je još `PUBLISHED`: log pokazuje drenažu result listenera, zatim "Closed
   1 open status subscription(s)", pa Tomcat-ov graceful shutdown koji se
   odmah završava jer nema više šta da čeka; klijent vidi čist kraj
   chunked odgovora (`curl -v` prijavljuje da je konekcija ostala
   netaknuta, bez reset-a).

## Pretpostavke

- Baza podataka nije potrebna niti se koristi - oba servisa smeju da drže
  stanje in-memory prema opisu zadatka, pa se ono resetuje pri restartu
  (nivoi zaliha, zapamćeni ishodi obrađenih narudžbina, same narudžbine, kao
  i Redis Streams ako se ukloni `redis-data` volume). Ovo je i razlog zašto
  gubitak narudžbina u `order-service`-u pri restartu čini drenirane result
  poruke neuparivim - sa pravom bazom isti kod postaje pravi recovery put.
- `order-service` validira `orderId`/`itemId` (ne sme biti prazno) i
  `quantity` (pozitivan ceo broj) i vraća `400 Bad Request` na neispravan
  unos, pre nego što bilo šta bude objavljeno.
- Pretpostavlja se jedna instanca `inventory-service`-a. Mehanika consumer
  group-e (i `consumerName` property) podržava skaliranje na više instanci
  radi paralelne obrade, ali to nije bio postavljeni zahtev i nije testirano
  pod opterećenjem.
- "Rezervacija" je simulirana: umanjuje in-memory brojač, ne modeluje
  rezervacije sa rokom (hold/expiry) niti kompenzacione tokove
  oslobađanja pri otkazivanju.
- SSE pretplate žive u jednoj instanci `order-service`-a koja drži konekciju.
  Sa više od jedne instance iza load balancer-a, klijent bi mogao da se
  pretplati na instancu koja nikad ne konzumira rezultat baš te narudžbine;
  da bi to radilo, bilo bi potrebno emitovati promene statusa ka svakoj
  instanci (Redis Pub/Sub fan-out iznad postojećeg trajnog stream-a) - van
  obima ovde, i navedeno kao ograničenje, ne prećutano.
- SSE konekciju server zatvara nakon `order.status-stream.timeout-ms`
  (podrazumevano 5 min) ako narudžbina do tada nije odlučena; standardni
  `EventSource` klijent se rekonektuje i dobija svež snapshot.

## Napomena o korišćenju AI alata

AI alati (Claude) su korišćeni tokom celog razvoja - kako je i dozvoljeno
opisom zadatka - za skafolding, implementaciju i pisanje ovog README-a.
Arhitektonske odluke (Redis Streams naspram Pub/Sub-a, sibling-folder
struktura repozitorijuma, retry/DLQ dizajn) su usmeravane i pregledane od
mene, i u stanju sam da na razgovoru provedem kroz obrazloženje svake od
njih.
