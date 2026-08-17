# Kontekst za Claude Code — MDS tehnički zadatak (Order API + Inventory)

Ovo je sažetak svega dogovorenog u prethodnoj sesiji sa Claude (Cowork), pripremljen da
Claude Code odmah nastavi bez ponovnog objašnjavanja. Sačuvaj ovaj fajl kao `CLAUDE.md` u
root-u repozitorijuma (`F:\projects\MDS_konkurs\demo\CLAUDE.md`) — Claude Code ga automatski
učitava kao project memory na početku svake sesije u tom folderu.

## Ko sam ja (kandidat)

Java developer, ~15 godina iskustva, fokus na Java/Spring Boot/Spring framework-i, Keycloak,
MongoDB, MSSQL, mikroservisi. Na poslu koristim Redis Streams kao message broker.

## Zadatak (iz oglasa za posao)

Implementirati mali event-driven sistem od **dva nezavisna servisa** koji komuniciraju preko
event broker-a. **Fokus evaluacije: kvalitet i pouzdanost integracije, ne poslovna logika.**

1. **Order API servis** — izlaže `POST /orders`, payload `{orderId, itemId, quantity}`, po
   pozivu objavljuje event brokeru.
2. **Inventory Processing Service** — konzumira te evente, simulira rezervaciju zaliha
   (dovoljna količina → uspeh, inače → odbijanje). Čuvanje u memoriji je OK, baza nije potrebna.

Tehnički zahtevi: Java + Spring framework, UI nije potreban, **open-source broker** (cloud-managed
messaging servisi NISU dozvoljeni), projekat mora biti dockerizovan i pokretljiv lokalno preko
Docker Compose / Podman Compose. Očekivano vreme: 5–8h — ne preterivati sa doterivanjem nebitnih
delova, više se ceni jasnoća i inženjerske odluke nego veličina projekta. AI alati su dozvoljeni,
ali moram da budem u stanju da objasnim svaku odluku na tehničkom razgovoru.

Predaja: link ka javnom GitHub repozitorijumu + README sa uputstvom za pokretanje + napomene o
pretpostavkama.

## Arhitektonske odluke (već donete, ne preispitivati bez razloga)

**Broker: Redis Streams** (ne Pub/Sub — Pub/Sub nema perzistenciju ni redelivery, ne bi
zadovoljio "pouzdanost integracije"). Razlog izbora: ovo je pattern koji kandidat koristi u
produkciji na poslu, pa može autentično da ga objasni na razgovoru.

- Docker image: **`redis:8-alpine` ili noviji** (obavezno — Redis je do verzije 8 bio pod
  RSALv2/SSPLv1 licencom koja NIJE OSI-odobrena open-source licenca; tek od Redis 8, maj 2025,
  vraćena je AGPLv3 opcija koja jeste OSI-odobrena. Starije verzije NE zadovoljavaju zahtev
  zadatka "open-source message broker".)
- `redis-server --appendonly yes` u docker-compose — AOF perzistencija, da stream preživi
  restart kontejnera.
- Stream: `orders-stream`. Consumer group: `inventory-service-group`.
- Pouzdanost: `XREADGROUP` + ručni `XACK` tek nakon uspešne obrade (exception → poruka ostaje u
  Pending Entries List-i, nije izgubljena).
- Idempotencija: Inventory servis čuva `Set<String>` (npr. `ConcurrentHashMap.newKeySet()`)
  obrađenih `orderId` vrednosti u memoriji — duplirana isporuka se prepoznaje i preskače (ali se
  i dalje ack-uje).
- Retry / DLQ ekvivalent: scheduled zadatak (`@Scheduled`) periodično proverava `XPENDING` za
  poruke starije od praga (npr. 30s) koje nisu potvrđene, radi `XCLAIM` + ponovni pokušaj do N
  puta, nakon čega ih prebacuje (`XADD`) u `orders-stream-dlq` i loguje kao trajno neuspele.
- Spring zavisnost: `spring-boot-starter-data-redis` (koristi `StringRedisTemplate` i
  `StreamMessageListenerContainer`).

**Struktura repozitorijuma: dva nezavisna sibling foldera, BEZ parent pom-a.**


```
demo/                          (git repo root)
├── order-service/                 (nezavisan Maven projekat)
├── inventory-service/         (nezavisan Maven projekat)
├── docker-compose.yml         (root nivo — pokreće redis + oba servisa)
└── README.md                  (root nivo — uputstvo za pokretanje + napomene)
```

**Paketna konvencija (kandidatova navika sa posla — horizontalna/slojevita podela)**:
`model` (entiteti), `dto` (request/response), `dao`/`repository` (persistencija — ovde in-memory),
`services` (poslovna logika), `web` (kontroleri/API), `config` (konfiguracija/beans), plus
poseban paket za svaku specifičnu tehnološku integraciju (kod kandidata na poslu: `redis`,
`feign`; ovde: **`messaging`** za Redis Streams integraciju).

## Trenutno stanje projekta (F:\projects\MDS_konkurs\demo)

- Git repo već inicijalizovan, grane `main` i `develop`, remote `origin` postoji (GitHub).
  Kandidat je trenutno na `develop` grani. Koristi **GitHub Desktop** za commit/push (ne git CLI).
- Postojeći (nerestrukturiran) projekat generisan sa start.spring.io:
  - `groupId`: `com.lvl.mds`, `artifactId`: `demo`, `package`: `com.lvl.mds.demo`
  - `spring-boot-starter-parent` verzija **4.1.0**, `java.version` **25**
  - Zavisnosti: `spring-boot-starter-webmvc` (napomena: Spring Boot 4 preimenovao je stari
    `spring-boot-starter-web` — proveri da li je ovo tačan artifact ID u trenutnoj verziji BOM-a
    pre nego što nastaviš, moguće da treba `spring-boot-starter-web`), `spring-boot-starter-webmvc-test`
  - `src/main/java/com/lvl/mds/demo/DemoApplication.java` — prazna Spring Boot main klasa
  - `src/main/java/com/lvl/mds/demo/request.http` — već sadrži test POST /orders poziv (IntelliJ
    HTTP client), zadržati/premestiti kao referencu za ručno testiranje
  - Maven wrapper (`mvnw`/`mvnw.cmd`) prisutan, Maven NIJE globalno instaliran (namerno, koristi
    se wrapper ili IDE-ov bundled Maven)
- **URAĐENO**: `.gitignore` ažuriran da isključi `target/` i `.idea/` (bili su greškom praćeni u
  git-u).
- **NEZAVRŠENO — prvi sledeći korak**: komanda `git rm -r --cached target .idea` nije uspela
  zbog zaglavljenog `.git/index.lock` fajla (sada je ručno obrisan od strane kandidata) — pokreni
  ponovo `git rm -r --cached target .idea` da se `target/` i `.idea/` stvarno uklone iz git indexa
  (fajlovi ostaju lokalno, samo se prestaju pratiti).

## Šta treba da se uradi (redosled)

1. Dovrši git cleanup: `git rm -r --cached target .idea` u root-u repoa.
2. Napravi `order-service/` folder, `git mv` postojeći `pom.xml`, `src/`, `mvnw`, `mvnw.cmd`, `.mvn/`,
   `HELP.md` u njega (očuvaj git istoriju kroz `mv`, ne kopiranje).
3. U `order-service/pom.xml`: promeni `artifactId` u `order-service`, dodaj `spring-boot-starter-data-redis`.
4. Preimenuj paket `com.lvl.mds.demo` → `com.lvl.mds.orderapi` (folder struktura + package
   deklaracije), `DemoApplication` → `OrderApiApplication`, isto za test klasu.
5. Implementiraj u `order-api`: `web.OrdersController` (`POST /orders`), `dto.OrderRequest`,
   `messaging.OrderEventPublisher` (XADD u `orders-stream`), `config` klasa za Redis konekciju.
6. Napravi `inventory-service/` kao nov nezavisan Maven projekat (Spring Boot 4.1.0, Java 25,
   `spring-boot-starter`, `spring-boot-starter-data-redis`), `groupId` isti (`com.lvl.mds`),
   `artifactId` `inventory-service`, paket `com.lvl.mds.inventoryservice`, main klasa
   `InventoryServiceApplication`.
7. Implementiraj u `inventory-service`: `model.InventoryItem`, `repository` (in-memory mapa
   `itemId → quantity`, sa nekim seed podacima pri startu — npr. preko `CommandLineRunner`),
   `services.InventoryService` (logika rezervacije), `messaging.OrderEventConsumer`
   (`StreamMessageListenerContainer`, ručni `XACK`, idempotency set), `messaging` scheduled
   reclaim/retry/DLQ zadatak, `config` klasa.
8. `docker-compose.yml` u root-u: `redis:8-alpine` (sa `--appendonly yes`), `order-api` i
   `inventory-service` servisi (svaki sa svojim `Dockerfile`, multi-stage build: Maven build stage
   + slim JRE runtime stage na bazi Eclipse Temurin 25).
9. `README.md` u root-u: uputstvo za pokretanje (`docker compose up --build`), kratak opis
   arhitekture i odluka (uključujući objašnjenje zašto Redis Streams a ne Pub/Sub, zašto sibling
   folderi a ne multi-module), napomena o pretpostavkama, napomena da je AI korišćen u razvoju
   (task to eksplicitno traži).
10. Testiranje: proveri ceo flow kroz `docker compose up --build`, pošalji test POST zahtev
    (postoji već `request.http`), proveri da se poruka konzumuje i ack-uje, testiraj i scenario
    nedovoljne količine (odbijanje) i scenario pada Inventory servisa nasred obrade (poruka mora
    ostati u PEL-u i biti reklamovana).
11. Osnovni testovi (nice-to-have u okviru vremenskog budžeta): idempotency skip, insuficient
    quantity rejection.

## Napomene / ograničenja

- Ne preterivati sa vremenom — cilj je jasnoća i dobre inženjerske odluke, ne veličina projekta
  (5–8h okvir).
- NE prenositi stvarni kod/strukturu sa trenutnog poslodavca kandidata — samo opšte inženjerske
  konvencije opisane iznad (kandidat je to eksplicitno tražio da se poštuje).
- Kandidat radi u IntelliJ IDEA Community (probno, prelazi sa Spring Tool Suite/Spring Tools 5) i
  koristi GitHub Desktop za git operacije — izbegavati predloge koji pretpostavljaju da će sam
  kucati git CLI komande za svakodnevni workflow (mada ih Claude Code može izvršavati direktno).
