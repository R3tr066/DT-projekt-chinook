# **ETL proces datasetu Chinook**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **Chinook** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich hudobných preferencií na základe počtu predajov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa hudby, sťahovaní a používateľov. Táto analýza umožňuje identifikovať trendy v hudbe, najpopulárnejších žánrov a správanie používateľov.

Zdrojové dáta pochádzajú z Chinook datasetu dostupného [tu](https://github.com/lerocha/chinook-database/tree/master). Dataset obsahuje jedenásť hlavných tabuliek:
- `Track`
- `InvoiceLine`
- `Invoice`
- `Customer`
- `Employee`
- `Genre`
- `MediaType`
- `PlaylistTrack`
- `Playlist`
- `Album`
- `Artist`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/R3tr066/DT-projekt-chinook/blob/master/Chinook%20(MySQL)/Chinook_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma Chinook</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`invoiceLineFact`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dimEmployee`**: Obsahuje podrobné informácie o zamestnancoch (meno, titul, adresa, číslo manažéra).
- **`dimTrack`**: Obsahuje informácie o pesničkách (Názov, album, žáner)
- **`dimInvoice`**: uchováva informácie o faktúrach (číslo zakazníka, dátum, cena celkovo)
- **`dimCustomer`**: Obsahuje infomácie o zákaznikoch (Meno, adresa, mobil, email)
-  **`dimDate`**: Obsahuje podrobné časové údaje (hodina, deň, mesiac, rok).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/R3tr066/DT-projekt-chinook/blob/master/Chinook%20(MySQL)/chinook_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre Chinook</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE LYNX_PROJEKT_STG;
```
Do stage boli následne nahraté súbory obsahujúce údaje o pesničkách, používateľoch, faktúrach, zamestnancoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO Album
FROM @LYNX_PROJEKT_STG/Album.csv
FILE_FORMAT = csv
ON_ERROR = CONTINUE;
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.1 Transfor (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. `dimInvoice` obsahuje údaje o faktúrach, zákaznikoch, dátumu faktúry a celkovej sumy.  Táto dimenzia je typu SCD 0.
```sql
CREATE TABLE dimInvoice AS
SELECT
    i.`INVOICEID` as dim_invoiceID,
    i.`CUSTOMERID` as dim_customerID,
    CAST(i.`INVOICEDATE` AS DATE) as invoiceDate,
    i.`BILLINGADDRESS` as dim_billingAddress,
    i.`TOTAL` as dim_total
FROM INVOICE i;
```
Dimenzia `dimCustomer` je navrhnutá tak, aby uchovávala informácie o zákaznikoch. Obsahuje odvodené údaje, ako sú meno, priezvisko, dátum narodenia. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 1. To znamená, že existujúce záznamy v tejto dimenzii sú menné.
```sql
CREATE TABLE dimCustomer AS
SELECT DISTINCT
    c.`CUSTOMERID` AS dim_customerID,
    c.`FIRSTNAME` as dim_firstName,
    c.`LASTNAME` as dim_lastName,
    c.`ADDRESS` as dim_address,
    c.`PHONE` as dim_phone,
    c.`EMAIL` as dim_email,
    c.`SUPPORTREPID` as dim_supportRepID
FROM CUSTOMER c;
```
Dimenzia `dimDate` obsahuje údaje o dátumoch a časoch, ako sú deň, mesiac, rok. Táto dimenzia je typu SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.
```sql
CREATE TABLE dimDate AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(`INVOICEDATE` AS DATE)) AS dim_dateID,
    CAST(`INVOICEDATE` AS DATE) AS date,
    DATE_PART(day, `INVOICEDATE`) AS day,
    DATE_PART(dow, `INVOICEDATE`) + 1 AS dayOfWeek,
    CASE DATE_PART(dow, `INVOICEDATE`) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS dayOfWeekAsString,
    DATE_PART(month, `INVOICEDATE`) AS month,
    DATE_PART(year, `INVOICEDATE`) AS year,
    DATE_PART(quarter, `INVOICEDATE`) AS quarter
FROM INVOICE;
```
Dimenzia `dimTrack` uchováva informácie o pesničkách ako sú napríklad názov, album, žáner a typ média. Táto dimenzia je SCD typ 2, kedy je možné sledovať cenu pesničky.

```sql
CREATE TABLE dimTrack AS
SELECT
    t.`TRACKID` as dim_trackID,
    t.`NAME` as dim_name,
    COALESCE(a.`TITLE`, 'Unknown') as dim_album,
    COALESCE(g.`NAME`, 'Unknown') as dim_genre,
    t.`UNITPRICE` as dim_price,
    t.`MILLISECONDS` as dim_length,
    COALESCE(ar.`NAME`, 'Unknown') as dim_artist,
    mt.`NAME` as dim_mediaType
FROM TRACK t
LEFT JOIN GENRE g ON t.`GENREID` = g.`GENREID`
LEFT JOIN ALBUM a ON t.`ALBUMID` = a.`ALBUMID`
LEFT JOIN ARTIST ar ON a.`ARTISTID` = ar.`ARTISTID`
JOIN MEDIATYPE mt ON t.`MEDIATYPEID` = mt.`MEDIATYPEID`;
```
Tabuľka `dimEmployee` obsahuje informácie o zamestnancoch.  Táto tabuľka je SCD typ 2 pre uchovanie adresy, emailu a čísla manažéra.
```sql
CREATE TABLE dimEmployee AS
SELECT
    e.`EMPLOYEEID` as dim_employeeID,
    e.`FIRSTNAME` as dim_firstName,
    e.`LASTNAME` as dim_lastName,
    e.`TITLE` as dim_title,
    e.`REPORTSTO` as dim_managerID,
    e.`PHONE` as dim_phone,
    e.`EMAIL` as dim_email,
    e.`ADDRESS` as dim_address,
    e.`BIRTHDATE` as dim_dateOfBirth
FROM EMPLOYEE e;
```


Faktová tabuľka `InvoiceLineFact` obsahuje záznamy o faktúrach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je cena a počet kusov.
```sql
CREATE TABLE InvoiceLineFact AS
SELECT
    il.`InvoiceLineId` AS invoiceLineFactID,
    il.`UnitPrice` AS dim_unitPrice,
    il.`Quantity` AS dim_quantity,
    t.DIM_TRACKID AS trackID,
    i.DIM_INVOICEID AS invoiceID,
    i.DIM_CUSTOMERID AS customerID,
    e.DIM_EMPLOYEEID AS employeeID,
    d.DIM_DATEID AS dateID
FROM InvoiceLine il
JOIN DIMTRACK t ON il.`TRACKID` = t.DIM_TRACKID
JOIN DIMINVOICE i ON il.`INVOICEID` = i.DIM_INVOICEID
JOIN DIMCUSTOMER c ON i.DIM_CUSTOMERID = c.DIM_CUSTOMERID
JOIN DIMEMPLOYEE e ON c.DIM_SUPPORTREPID = e.DIM_EMPLOYEEID
JOIN DIMDATE d ON i.INVOICEDATE = d.DATE;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE ALBUM;
DROP TABLE ARTIST;
DROP TABLE CUSTOMER;
DROP TABLE EMPLOYEE;
DROP TABLE GENRE;
DROP TABLE INVOICELINE;
DROP TABLE INVOICE;
DROP TABLE MEDIATYPE;
DROP TABLE PLAYLIST;
DROP TABLE PLAYLISTTRACK;
DROP TABLE TRACK;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu hudobných preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa hudby. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/R3tr066/DT-projekt-chinook/blob/master/Chinook%20(MySQL)/chinook%20_dashboard.png" alt="chinook dashboard">
  <br>
  <em>Obrázok 3 Dashboard Chinook datasetu</em>
</p>

---
### **1. Denný predaj (tržby) podľa dní v týždni**
Táto vizualizácia zobrazuje dni v týždni a ich tržby. Umožňuje identifikovať v ktorý deň v týždni bolo najviac predajov. Zistíme že v piatok je najviac stiahnutí.

```sql
SELECT 
    d.dayOfWeekAsString AS "Day of Week",
    SUM(i.dim_total) AS "Total Sales"
FROM dimDate d
JOIN dimInvoice i 
  ON d.date = i.invoiceDate
GROUP BY d.dayOfWeekAsString
ORDER BY 
    CASE d.dayOfWeekAsString
        WHEN 'Pondelok' THEN 1
        WHEN 'Utorok' THEN 2
        WHEN 'Streda' THEN 3
        WHEN 'Štvrtok' THEN 4
        WHEN 'Piatok' THEN 5
        WHEN 'Sobota' THEN 6
        WHEN 'Nedeľa' THEN 7
    END;
```
---
### **2. Top 5 zákazníkov podľa celkových tržieb**
Graf znázorňuje TOP 5 zákaznikov podľa počtu tržieb. V grafe sme zistili že najviac tržieb mala Helena Holý.

```sql
SELECT 
    CONCAT(c.dim_firstName, ' ', c.dim_lastName) AS "Customer Name",
    SUM(i.dim_total) AS "Total Sales"
FROM dimCustomer c
JOIN dimInvoice i 
  ON c.dim_customerID = i.dim_customerID
GROUP BY "Customer Name"
ORDER BY "Total Sales" DESC
LIMIT 5;
```
---
### **3. Predaje podľa žánrov (genre)**
Graf ukazuje predaje podľa žánru piesne. Z grafu sme zistili že najväčší záujem je o piesne žánru Rock

```sql
SELECT 
    t.dim_genre AS "Genre",
    SUM(il.dim_unitPrice * il.dim_quantity) AS "Total Sales"
FROM dimTrack t
JOIN InvoiceLineFact il 
  ON t.dim_trackID = il.trackID
GROUP BY t.dim_genre
ORDER BY "Total Sales" DESC;
```
---
### **4. Výkonnosť zamestnancov podľa počtu obslúžených zákazníkov**
Tabuľka znázorňuje, ako zamestnanci pracujú a tu sú najlepší z nich. Z grafu sme zistili že Jane Peacock obslúžila 21 zákaznikov

```sql
SELECT 
    CONCAT(e.dim_firstName, ' ', e.dim_lastName) AS "Employee Name",
    COUNT(DISTINCT c.dim_customerID) AS "Customer Count"
FROM dimEmployee e
JOIN dimCustomer c 
  ON e.dim_employeeID = c.dim_supportRepID
GROUP BY "Employee Name"
ORDER BY "Customer Count" DESC;
```
---
### **5. Mesačný trend predaja**
Graf nám zobrazuje mesačné predaje počas rokov 2021 - 2025. Z grafu vieme zistiť že najviac predajov celkovo bolo v Júny.

```sql
SELECT 
    d.year AS "Year",
    d.month AS "Month",
    SUM(i.dim_total) AS "Total Sales"
FROM dimDate d
JOIN dimInvoice i 
  ON d.date = i.invoiceDate
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa hudobných preferencií . Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a hudobných služieb.

---

**Autor:** Tomáš Šablica
