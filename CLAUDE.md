# Kontekst za Claude Code — MDS tehnički zadatak (Order API + Inventory)

Kandidat: Java developer, ~15 godina iskustva, fokus Java/Spring Boot, Keycloak, MongoDB, MSSQL,
mikroservisi. Na poslu
koristi Redis Streams kao broker.

## Zadatak (iz oglasa za posao)

Event-driven sistem od dva nezavisna servisa preko event broker-a. Fokus evaluacije: kvalitet i
pouzdanost integracije, ne poslovna logika.

1. Order API — `POST /orders` `{orderId, itemId, quantity}`, objavljuje event brokeru.
2. Inventory Processing Service — konzumira evente, simulira rezervaciju zaliha (dovoljno →
   uspeh, inače → odbijanje). In-memory čuvanje je OK, baza NIJE potrebna.

Java + Spring, UI nije potreban, open-source broker (ne cloud-managed), dockerizovano
(Docker/Podman Compose). ~5-8h, ne preterivati sa doterivanjem. AI alati dozvoljeni, kandidat
mora da objasni svaku odluku na razgovoru. Predaja: link ka public GitHub repou + README +
napomene o pretpostavkama.

## STATUS: implementacija je urađena (10 commit-ova na `develop` grani, Co-Authored-By Claude Sonnet 5)

Struktura je sibling folderi (bez parent pom-a), kako je dogovoreno:

```
demo/                    (git repo root, grane main/develop, remote origin na GitHub-u)
├── order-service/        (Maven projekat — NAPOMENA: ime je order-service, ne order-api kako je
│                          prvobitno planirano — to je OK, samo napomena radi konzistentnosti)
├── inventory-service/    (Maven projekat)
├── docker-compose.yml    (redis + order-service + inventory-service)
├── README.md             (uputstvo za pokretanje + arhitektura + pretpostavke)
└── CLAUDE.md             (ovaj fajl)
```

Implementirano (potvrđeno kroz `find` i `git log`):
- `order-service`: `com.lvl.mds.orderapi` paket — `web.OrdersController`, `dto.OrderRequest`,
  `dto.OrderResponse`, `messaging.OrderEventPublisher`, `config.OrderStreamProperties`,
  `OrderApiApplication`, testovi (`OrderApiApplicationTests`, `OrdersControllerTest`).
- `inventory-service`: `com.lvl.mds.inventoryservice` paket — `model.InventoryItem`,
  `model.ReservationOutcome`, `repository.InventoryRepository`, `services.InventoryService`,
  `messaging.OrderEventConsumer`, `messaging.OrderEventProcessor`,
  `messaging.PendingMessagesReclaimer` (retry/reklamovanje preko XPENDING/XCLAIM → DLQ nakon N
  pokušaja), `messaging.ProcessedOrdersStore` (idempotency), `messaging.StreamInitializer`,
  `config.InventorySeedInitializer`, `config.RedisStreamListenerConfig`,
  `config.RedisStreamProperties`, `config.RetryProperties`, testovi za sve gore navedeno.
- **Povratna sprega o rezervaciji (backlog stavka 1) — DONE, 2026-08-18.**
  `inventory-service` objavljuje ishod na `order-results-stream`
  (`messaging.ReservationResultPublisher`), `order-service` ga konzumira preko
  consumer grupe `order-service-group` (`messaging.ReservationResultConsumer`,
  `ReservationResultProcessor`, `ResultStreamInitializer`,
  `config.ResultStreamListenerConfig`). `OrderStatus` prosiren sa `RESERVED`,
  `REJECTED_INSUFFICIENT_STOCK`, `REJECTED_UNKNOWN_ITEM`, `FAILED` + `isTerminal()`,
  `Order` dobio `statusReason`/`updatedAt`. `ProcessedOrdersStore` je sada
  `Map<orderId, ReservationOutcome>` (duplikat ne rezervise ponovo ali PONOVO
  objavljuje ishod), `PendingMessagesReclaimer` emituje `FAILED` kad salje u DLQ.
  Opcioni deo: `GET /orders/{orderId}/status` kao SSE
  (`web.OrderStatusStream` + `services.OrderStatusChangedEvent`, in-JVM Spring event
  da web sloj zavisi od servisnog, a ne obrnuto).
- **Usput ispravljen jos jedan realan bag:** `InventorySeedInitializer` je bio
  `CommandLineRunner`, koji se izvrsava TEK posle refresh-a konteksta — a stream
  listener container krece da konzumira ranije. Pri restartu sa zaostalim porukama
  na `orders-stream` narudzbine su obradjivane nad praznim inventarom i odbijane kao
  `ITEM_NOT_FOUND`. Sada je `@PostConstruct` + `@DependsOn` na listener container-u.
  Otkriveno tek kroz ovu stavku, jer je rezultat ranije zivio samo u logu.

- **Graceful shutdown (backlog stavka 5) — DONE, 2026-08-18.** Novi
  `messaging.StreamListenerLifecycle` u oba servisa (ista klasa, kopirana kao
  event contract) preuzima start/stop `StreamMessageListenerContainer`-a kao
  `SmartLifecycle` na fazi `Integer.MAX_VALUE`: `container.stop()` sam po sebi
  samo obori flag i vrati se odmah, dok poll thread moze biti usred
  `onMessage()` — a Redis connection factory (Lettuce) gasi se tek na fazi
  `0`, pa bi trka do zatvaranja konteksta mogla srusiti zavrsni `XACK` na
  vec mrtvoj konekciji. `stop()` zato ceka da `Subscription.isActive()`
  postane `false` (dokaz da je poll thread stvarno izasao iz event loop-a),
  ograniceno sa `{service}.shutdown.listener-drain-timeout-ms`. Preseljenje
  `container.start()` iz `@Bean` metode u lifecycle fazu je usput ucvrstilo i
  `@DependsOn` redosled iz `InventorySeedInitializer` bug-a strukturno (svaki
  singleton je konstruisan pre bilo kog `start()`-a), a ne samo napomenom u
  javadoc-u. `web.OrderStatusStream` je takodje `SmartLifecycle`, jedna faza
  iznad `WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE`: zatvara sve
  otvorene SSE konekcije PRE nego sto Tomcat (uz `server.shutdown=graceful`)
  pocne da ceka na aktivne requestove — inace bi jedan klijent koji gleda
  narudzbinu na cekanju blokirao svaki shutdown do isteka
  `spring.lifecycle.timeout-per-shutdown-phase`. `inventory-service` dodatno
  ima `spring.task.scheduling.shutdown.await-termination=true` jer
  `PendingMessagesReclaimer` radi `XCLAIM` → obradi → `XACK` kao jednu
  celinu — prekid usred tog prolaza bi ostavio poruku "zaklajmovanu" od
  konzumera koji vise ne postoji. `docker-compose.yml` ima
  `stop_grace_period: 30s` na oba servisa (Compose default 10s je krace od
  `timeout-per-shutdown-phase`=20s, pa bi SIGKILL sigurno stigao nasred
  drenaze). Verifikovano uzivo kroz `docker compose`: gasenje
  `inventory-service`-a sa neobradjenom porukom ide "Stopping stream
  listener" → "drained" za ~1s, `XPENDING` posle toga prazan; gasenje
  `order-service`-a dok `curl -N .../status` drzi otvorenu konekciju na
  narudzbini koja jos nije `RESERVED` pokazuje redosled: listener drain →
  "Closed 1 open status subscription(s)" → Tomcat-ov graceful shutdown koji
  odmah zavrsi jer nema vise sta da ceka; `curl -v` na klijentu potvrdjuje
  cist kraj chunked odgovora (`Connection ... left intact`, bez reset-a), a
  ceo shutdown traje ~1s naspram 30s grace perioda.

- Dockerfile-ovi za oba servisa, root `docker-compose.yml` (redis:8-alpine +
  `--appendonly yes` + oba servisa).
- README.md sa uputstvom i arhitekturom.
- Usput ispravljen realan bag: pogrešna detekcija BUSYGROUP greške pri restartu servisa
  (consumer group već postoji u Redis-u nakon restarta — sad se to ispravno hvata).

## Arhitektonske odluke (zaključane, ne preispitivati bez razloga)

**Broker: Redis Streams** (ne Pub/Sub — nema perzistenciju/redelivery). Razlog: pattern koji
kandidat koristi u produkciji, može autentično da ga objasni na razgovoru.

- Docker image `redis:8-alpine` ili noviji — OBAVEZNO, jer je tek od Redis 8 (maj 2025) vraćena
  AGPLv3 (OSI-odobrena open-source licenca); starije verzije su pod RSALv2/SSPLv1 koje NISU
  OSI-odobrene, ne bi zadovoljile zahtev "open-source broker".
- `redis-server --appendonly yes` — AOF perzistencija.
- Stream `orders-stream`, consumer group `inventory-service-group`.
- `XREADGROUP` + ručni `XACK` posle uspešne obrade; exception → poruka ostaje u PEL-u.
- Idempotencija: in-memory `Set<String>` obrađenih `orderId` (ProcessedOrdersStore).
- Retry/DLQ: `@Scheduled` proverava `XPENDING` za poruke starije od praga, `XCLAIM` + retry do N
  puta, zatim `XADD` u `orders-stream-dlq`.

**Povratna sprega: drugi stream u suprotnom smeru** (`order-results-stream`), ne HTTP
callback (vratio bi sinhrono spregu koju broker upravo uklanja) i ne deljeni Redis kljuc
koji `order-service` anketira (nema redosleda, redelivery-ja ni backlog-a). Redosled u
`OrderEventProcessor` je namerno: rezervisi → upamti ishod lokalno → `XADD` rezultat →
`XACK` narudzbinu; ako `XADD` pukne, poruka ostaje u PEL-u i redelivery ponovo objavi
zapamceni ishod umesto da rezervise dvaput. `order-service` na startu drenira sopstveni
PEL (`ResultStreamInitializer`), jer live listener cita samo neisporucene poruke.

**SSE, ne WebSocket** za `GET /orders/{orderId}/status`: saobracaj je jednosmeran, obican
HTTP bez upgrade-a, `EventSource` se sam rekonektuje, a veza se zatvara cim narudzbina
udje u terminalno stanje (ogranicena pretplata, ne otvoreni kanal). Prvo se salje trenutni
snapshot pa tek onda promene — zato je endpoint bezbedan za poziv u bilo kom trenutku.


**Struktura repoa: dva nezavisna sibling foldera, BEZ parent pom-a.** Multi-module Maven je
namerno odbijen (veštački bi vezao build dva servisa koja treba da budu nezavisna). Svaki servis
ima svoju kopiju event contract-a (DTO), ne dele Java kod.

**Paketna konvencija (kandidatova navika sa posla, horizontalna/slojevita)**: `model`, `dto`,
`dao`/`repository`, `services`, `web`, `config`, plus poseban paket za specifičnu tehnološku
integraciju (`messaging` ovde — analogno kandidatovim `redis`/`feign` paketima na poslu).

## MongoDB — RAZMATRANO I ODBAČENO, ne implementirati

Kandidat je razmatrao dodavanje MongoDB-a kao audit log-a za obrađene porudžbine, ali je odlučio
da **ne** ga dodaje — zadatak eksplicitno kaže da baza nije potrebna, in-memory je dovoljan, i
dodatna baza bi bila scope creep bez jasne veze sa fokusom evaluacije (pouzdanost integracije
preko broker-a). Projekat ostaje isključivo na in-memory stanju (InventoryRepository) i in-memory
idempotency Set-u (ProcessedOrdersStore). Ne predlagati ponovo dodavanje baze osim ako kandidat
to eksplicitno ponovo ne zatraži.

## Preostalo / za proveru

Sve stavke ispod su naknadno proverene i potvrđene (2026-08-17, ista sesija u kojoj je ovaj
fajl izmenjen):

1. ✅ `.scratch_jar3/` (prazan, nepraćen folder od ranijeg debug-a `MapRecord` API-ja preko
   `javap`) — obrisan iz radnog direktorijuma. Nikad nije bio praćen u git-u (potvrđeno preko
   `git status --porcelain --ignored`), pa nije uticao na repo.
2. ✅ `.gitignore` ažuran, `target/`/`.idea/` potvrđeno untracked u oba servisa
   (`git ls-files | grep -E "(^|/)(target|\.idea)/"` → prazan rezultat).
3. ✅ End-to-end ponovo potvrđeno kroz `docker compose up --build`: happy path (rezervacija),
   nedovoljna količina (odbijanje), i scenario pada Inventory servisa nasred obrade — poruka
   ubačena u PEL preko ručnog `XREADGROUP` sa "ghost" konzumerom koji nikad ne radi `XACK`,
   pa je `inventory-service` restartovan i `PendingMessagesReclaimer` je poruku preuzeo
   (`XCLAIM`) i uspešno obradio nakon isteka praga — potvrđeno i u logovima i preko `XPENDING`
   (0 nakon reklamovanja).
4. ✅ README pregledan — sadrži uputstvo za pokretanje, sve četiri arhitektonske odluke
   (Redis Streams vs Pub/Sub, mehanizam pouzdanosti, sibling folderi vs multi-module, paketna
   konvencija), test scenarije, pretpostavke i napomenu o AI korišćenju. Nije menjan jer je već
   tačan.
5. Kandidat mora biti u stanju da objasni SVAKI deo koda na tehničkom razgovoru — sledeći
   prioritet nije više pisanje novog koda, već da kandidat prođe kroz postojeći kod i razume ga
   do detalja (redosled: messaging paket u oba servisa, pa PendingMessagesReclaimer logika, pa
   docker-compose/Dockerfile odluke). Ovo je jedina preostala, trajno otvorena stavka — nije
   nešto što Claude Code radi umesto kandidata.

6. Backlog produkcijskih mehanizama (memorija `mds-production-grade-backlog`, jedna
   stavka po sesiji): stavka 1 (povratna sprega + SSE) je zavrsena 2026-08-18 i
   verifikovana end-to-end kroz `docker compose` — happy path, nedovoljna kolicina,
   nepoznat artikal, duplikat (ponovno objavljivanje ishoda bez duple rezervacije),
   SSE preko restarta konzumera, i ceo lanac retry → DLQ → `FAILED` sa poison porukom.
   Stavka 2 (event envelope + versioning) je zavrsena u paralelnoj sesiji istog dana
   (jos nije komitovano u trenutku pisanja ove napomene). Stavka 5 (graceful shutdown)
   je takodje zavrsena 2026-08-18, opisana iznad. Napomena o konkurentnosti: stavke 2 i
   5 su rađene u dve odvojene sesije nad istim working tree-om istovremeno — kandidat je
   to primetio i eksplicitno zatrazio da se pauzira dok se stavka 2 ne zavrsi, pre nego
   sto je stavka 5 nastavljena i zavrsena. Nista nije izgubljeno (diff-ovi provereni), ali
   to znaci da radni metod "jedna stavka po sesiji, odvojene sesije" iz backlog memorije
   nije bio strogo ispostovan ovog dana. Preostale stavke (3, 4, 6-12) i dalje cekaju,
   redosled izmedju njih nije fiksiran.

## Napomene / ograničenja

- Ne preterivati sa vremenom — jasnoća i inženjerske odluke > veličina projekta.
- NE prenositi stvarni kod/strukturu sa trenutnog poslodavca kandidata — samo opšte inženjerske
  konvencije (kandidat je to eksplicitno tražio).
- Kandidat koristi IntelliJ IDEA (Community) i GitHub Desktop za git operacije (ne git CLI za
  svakodnevni rad, mada Claude Code može git komande izvršavati direktno).
