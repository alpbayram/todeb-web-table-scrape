import { Client, Databases, ID } from "node-appwrite";

// =====================
//  CONFIG (.env'den)
// =====================

const APPWRITE_ENDPOINT = process.env.APPWRITE_ENDPOINT;
const APPWRITE_PROJECT_ID = process.env.APPWRITE_PROJECT_ID;
const APPWRITE_API_KEY = process.env.APPWRITE_API_KEY;
const APPWRITE_DATABASE_ID = process.env.DATABASE_ID;
const APPWRITE_COLLECTION_ID = process.env.COLLECTION_ID;

// Mail atan Appwrite Function endpoint’in
const MAIL_FUNCTION_URL = "https://6909b832001efa359c90.fra.appwrite.run";

// =====================
//  APPWRITE CLIENT
// =====================

function createClient() {
    const client = new Client()
        .setEndpoint(APPWRITE_ENDPOINT)
        .setProject(APPWRITE_PROJECT_ID)
        .setKey(APPWRITE_API_KEY);

    const databases = new Databases(client);

    return { client, databases };
}

// =====================
//  DB'DEN VERİ ÇEK (oldData)
// =====================

async function getDbData(databases, dbCollection) {
    const response = await databases.listDocuments(
        APPWRITE_DATABASE_ID,
        dbCollection
    );

    const dbData = response.documents.map(doc => ({
        docId: doc.$id,
        kurulus_kodu: doc.kurulus_kodu,
        kurulus_adi: doc.kurulus_adi,
        yetkiler: doc.yetkiler || []
    }));

    return dbData;
}

// =====================
//  DISTILL PAYLOAD → newData
// =====================
//
// Distill body örneği:
// {
//   id, name, uri,
//   text: "[ { \"code\": \"898\", \"name\": \"...\", \"rights\": [\"a\", ...] }, ... ]"
// }

function mapDistillToNewData(distillPayload) {
    const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

    // text içindeki JSON string'i parse et
    const arr = JSON.parse(text);

    const newData = arr.map(item => ({
        kurulus_kodu: String(item.code).trim(),
        kurulus_adi: String(item.name).trim(),
        yetkiler: Array.isArray(item.rights) ? item.rights : []
    }));
    const trDate = new Date(ts).toLocaleString("tr-TR", {
        timeZone: "Europe/Istanbul",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
    });
    return {
        meta: { id, name, uri, trDate, to, dbCollection },
        newData
    };
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
    // DB'deki kodlar
    const oldCodes = oldData.map(item => item.kurulus_kodu);
    const oldCodesSet = new Set(oldCodes);

    // newData içinden, DB'de de olanlar
    const commonKuruluslar = newData.filter(item =>
        oldCodesSet.has(item.kurulus_kodu)
    );

    return commonKuruluslar;
}


function findChangedKuruluslar(commonKuruluslar, dbData) {
    const degisenler = [];

    for (let i = 0; i < commonKuruluslar.length; i++) {
        const item = commonKuruluslar[i]; // NEW DATA (Distill)
        const kod = item.kurulus_kodu;
        const yeniAdi = item.kurulus_adi;

        // DB'deki eski kayıt
        const dbRow = dbData.find(dbItem => dbItem.kurulus_kodu === kod);
        if (!dbRow) continue;

        const eskiAdi = dbRow.kurulus_adi;

        const isimDegistiMi = yeniAdi !== eskiAdi;

        if (isimDegistiMi) {
            // değişmişse newData'daki halini döndür
            degisenler.push(item);
        }
    }

    return degisenler;
}


function findChangedYetkiler(commonKuruluslar, dbData) {
    const degisenler = [];

    for (let i = 0; i < commonKuruluslar.length; i++) {
        const item = commonKuruluslar[i]; // NEW DATA
        const kod = item.kurulus_kodu;
        const yeniYetkiler = item.yetkiler || [];

        const dbRow = dbData.find(dbItem => dbItem.kurulus_kodu === kod);
        if (!dbRow) continue;

        const eskiYetkiler = dbRow.yetkiler || [];

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

    // degisenler1 + degisenler2 içindeki NEW data nesnelerini uniq hale getir
    const tumDegisenler = [...degisenler1, ...degisenler2];

    const uniqNewItems = [];
    const seen = new Set();

    for (let i = 0; i < tumDegisenler.length; i++) {
        const item = tumDegisenler[i];
        const kod = item.kurulus_kodu;

        if (!seen.has(kod)) {
            seen.add(kod);
            uniqNewItems.push(item);
        }
    }

    // degisenler3: eski + yeni değerleri birlikte döndürelim
    const degisenler3 = uniqNewItems.map(newItem => {
        const kod = newItem.kurulus_kodu;

        const oldItem = dbData.find(dbItem => dbItem.kurulus_kodu === kod) || {};

        return {
            kurulus_kodu: kod,

            // yeni
            kurulus_adi: newItem.kurulus_adi,
            yetkiler: newItem.yetkiler || [],

            // eski (db'den)
            kurulus_adi_eski: oldItem.kurulus_adi ?? null,
            yetkiler_eski: oldItem.yetkiler || []
        };
    });

    return {
        degisenler1,
        degisenler2,
        degisenler3
    };
}



// =====================
//  DB'Yİ newData İLE SENKRONLA
// =====================

async function syncDbWithNewData(databases, dbData, newData, removed, dbCollection) {
    const dbDataByCode = new Map(dbData.map(item => [item.kurulus_kodu, item]));

    // 1) Removed olanları sil
    for (let i = 0; i < removed.length; i++) {
        const item = removed[i];
        const existing = dbDataByCode.get(item.kurulus_kodu);

        if (existing && existing.docId) {
            await databases.deleteDocument(
                APPWRITE_DATABASE_ID,
                dbCollection,
                existing.docId
            );
        }
    }

    // 2) newData'yı DB'ye yaz (varsa update, yoksa create)
    for (let i = 0; i < newData.length; i++) {
        const item = newData[i];
        const existing = dbDataByCode.get(item.kurulus_kodu);

        const payload = {
            kurulus_kodu: item.kurulus_kodu,
            kurulus_adi: item.kurulus_adi,
            yetkiler: item.yetkiler
        };

        if (existing && existing.docId) {
            await databases.updateDocument(
                APPWRITE_DATABASE_ID,
                dbCollection,
                existing.docId,
                payload
            );
        } else {
            await databases.createDocument(
                APPWRITE_DATABASE_ID,
                dbCollection,
                ID.unique(),
                payload
            );
        }
    }
}

// =====================
//  MAIL FUNCTION ÇAĞIRMA
// =====================
//
// Şimdilik html'i basit tutuyoruz; sonra gerçek template'e çevirirsin.

async function sendReportMail({ meta, added, removed, changed }) {
    await fetch(MAIL_FUNCTION_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            to: meta.to,
            subject: "Güncelleme Raporu",
            meta,
            added,
            removed,
            changed
        })
    });
}


// =====================
//  ANA ÇALIŞTIRMA
// =====================
async function run(distillPayload) {
    // distillPayload → { meta, newData }
    const { meta, newData } = mapDistillToNewData(distillPayload);


    const { databases } = createClient();

    // oldData
    const dbData = await getDbData(databases, meta.dbCollection);




    // karşılaştırmalar
    const { added, removed } = compareKuruluslar(dbData, newData);
    const commonKuruluslar = getCommonKuruluslar(dbData, newData);
    const { degisenler3 } = kontrolEt(commonKuruluslar, dbData);

    // ---------------------------
    // MAIL FUNCTION'A GİDEN FORMAT
    // ---------------------------
    await sendReportMail({
        meta,
        added,
        removed,
        changed: degisenler3
    });

    // ---------------------------
    // DB'yi güncelle
    // ---------------------------
    await syncDbWithNewData(databases, dbData, newData, removed, meta.dbCollection);

    // Debug return (Appwrite logs)
    return {
        meta,
        added,
        removed,
        changed: degisenler3
    };
}


// =====================
//  APPWRITE FUNCTION HANDLER
// =====================

export default async ({ req, res, log, error }) => {
    try {
        const body =
            typeof req.body === "string" ? JSON.parse(req.body) : req.body ?? {};

        const result = await run(body);

        return res.json({
            success: true,
            ...result
        });
    } catch (err) {
        if (error) {
            error(err);
        } else {
            console.error(err);
        }

        return res.json({
            success: false,
            error: err.message ?? String(err)
        });
    }
};








