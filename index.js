// index.js

// Appwrite SDK
const { Client, Databases } = require("appwrite");
// HTML parse için (npm i cheerio)
const cheerio = require("cheerio");

// =====================
//  CONFIG
// =====================

const APPWRITE_ENDPOINT = "https://YOUR-ENDPOINT";     // ör: https://cloud.appwrite.io/v1
const APPWRITE_PROJECT_ID = "YOUR-PROJECT-ID";
const APPWRITE_DATABASE_ID = "YOUR-DATABASE-ID";
const APPWRITE_COLLECTION_ID = "YOUR-COLLECTION-ID";

// Web’den veri çekilecek URL
const WEB_URL = "https://ornek.site/kuruluslar.html";

// Mail atan Appwrite Function endpoint’in
const MAIL_FUNCTION_URL = "https://6909b832001efa359c90.fra.appwrite.run";

// =====================
//  APPWRITE CLIENT
// =====================

const client = new Client()
    .setEndpoint(APPWRITE_ENDPOINT)
    .setProject(APPWRITE_PROJECT_ID);

const databases = new Databases(client);

// =====================
//  DB'DEN VERİ ÇEK (oldData)
// =====================

async function getDbData() {
    const response = await databases.listDocuments(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID
    );

    const dbData = response.documents.map(doc => ({
        kurulus_kodu: doc.kurulus_kodu,
        kurulus_adi: doc.kurulus_adi,
        yetkiler: doc.yetkiler
    }));

    return dbData;
}

// =====================
//  WEB'DEN VERİ ÇEK (newData)
// =====================

async function getNewDataFromWeb() {
    const res = await fetch(WEB_URL);
    const html = await res.text();
    const $ = cheerio.load(html);

    const newData = [];

    // Buradaki selector'u kendi HTML yapına göre değiştir
    $("table#kuruluslar tbody tr").each((_, tr) => {
        const tds = $(tr).find("td");

        const kurulus_kodu = $(tds[0]).text().trim();
        const kurulus_adi = $(tds[1]).text().trim();
        const yetkilerText = $(tds[2]).text().trim(); // ör: "a, b, c"
        const yetkiler = yetkilerText
            .split(",")
            .map(y => y.trim())
            .filter(Boolean);

        if (kurulus_kodu) {
            newData.push({
                kurulus_kodu,
                kurulus_adi,
                yetkiler
            });
        }
    });

    return newData;
}

// =====================
//  KARŞILAŞTIRMA FONKSİYONLARI
// =====================

function compareKuruluslar(oldData, newData) {
    const oldCodes = new Set(oldData.map(item => item.kurulus_kodu));
    const newCodes = new Set(newData.map(item => item.kurulus_kodu));

    function isNewlyAddedCode(code) {
        return !oldCodes.has(code);
    }

    function isRemovedCode(code) {
        return !newCodes.has(code);
    }

    const added = newData.filter(item => isNewlyAddedCode(item.kurulus_kodu));
    const removed = oldData.filter(item => isRemovedCode(item.kurulus_kodu));

    return { added, removed };
}

function getCommonKuruluslar(oldData, newData) {
    const oldCodes = oldData.map(item => item.kurulus_kodu);
    const newCodes = newData.map(item => item.kurulus_kodu);

    const newCodesSet = new Set(newCodes);

    const commonCodes = oldCodes.filter(code => newCodesSet.has(code));

    const commonKuruluslar = oldData.filter(item => {
        return commonCodes.includes(item.kurulus_kodu);
    });

    return commonKuruluslar;
}

function findChangedKuruluslar(commonKuruluslar, dbData) {
    const degisenler = [];

    for (let i = 0; i < commonKuruluslar.length; i++) {
        const item = commonKuruluslar[i];

        const kod = item.kurulus_kodu;
        const yeniAdi = item.kurulus_adi;

        const dbRow = dbData.find(dbItem => dbItem.kurulus_kodu === kod);
        if (!dbRow) continue;

        const eskiAdi = dbRow.kurulus_adi;

        const isimDegistiMi = yeniAdi !== eskiAdi;

        if (isimDegistiMi) {
            degisenler.push(item);
        }
    }

    return degisenler;
}

function findChangedYetkiler(commonKuruluslar, dbData) {
    const degisenler = [];

    for (let i = 0; i < commonKuruluslar.length; i++) {
        const item = commonKuruluslar[i];

        const kod = item.kurulus_kodu;
        const yeniYetkiler = item.yetkiler;

        const dbRow = dbData.find(dbItem => dbItem.kurulus_kodu === kod);
        if (!dbRow) continue;

        const eskiYetkiler = dbRow.yetkiler;

        const yetkiDegistiMi =
            eskiYetkiler.length !== yeniYetkiler.length ||
            eskiYetkiler.some(y => !yeniYetkiler.includes(y)) ||
            yeniYetkiler.some(y => !eskiYetkiler.includes(y));

        if (yetkiDegistiMi) {
            degisenler.push(item);
        }
    }

    return degisenler;
}

function kontrolEt(commonKuruluslar, dbData) {
    const degisenler1 = findChangedKuruluslar(commonKuruluslar, dbData);
    const degisenler2 = findChangedYetkiler(commonKuruluslar, dbData);

    const tumDegisenler = [...degisenler1, ...degisenler2];

    const degisenler3 = [];
    const seen = new Set();

    for (let i = 0; i < tumDegisenler.length; i++) {
        const item = tumDegisenler[i];
        const kod = item.kurulus_kodu;

        if (!seen.has(kod)) {
            seen.add(kod);
            degisenler3.push(item);
        }
    }

    return {
        degisenler1,
        degisenler2,
        degisenler3
    };
}

// =====================
//  MAIL FUNCTION ÇAĞIRMA
// =====================

async function sendReportMail({ added, removed, changed }) {
    await fetch(MAIL_FUNCTION_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            added: added,
            removed: removed,
            changed: changed,
            to: "alp.bayram@todeb.org.tr",
            subject: "WebWatcher Güncelleme Raporu"
        })
    });
}

// =====================
//  ANA ÇALIŞTIRMA
// =====================

async function run() {
    const dbData = await getDbData();            // oldData
    const newData = await getNewDataFromWeb();   // newData (web tablosu)

    const { added, removed } = compareKuruluslar(dbData, newData);
    const commonKuruluslar = getCommonKuruluslar(dbData, newData);
    const { degisenler3 } = kontrolEt(commonKuruluslar, dbData);

    await sendReportMail({
        added,
        removed,
        changed: degisenler3
    });
}

// Appwrite Function içindeysen burayı handler’a göre uyarlarsın.
// Lokal script gibi çalıştırmak istersen:
run().catch(console.error);
