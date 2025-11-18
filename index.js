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

async function getDbData(databases) {
    const response = await databases.listDocuments(
        APPWRITE_DATABASE_ID,
        APPWRITE_COLLECTION_ID
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
    const { id, name, uri, text } = distillPayload;

    // text içindeki JSON string'i parse et
    const arr = JSON.parse(text);

    const newData = arr.map(item => ({
        kurulus_kodu: String(item.code).trim(),
        kurulus_adi: String(item.name).trim(),
        yetkiler: Array.isArray(item.rights) ? item.rights : []
    }));

    return {
        meta: { id, name, uri },
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
//  DB'Yİ newData İLE SENKRONLA
// =====================

async function syncDbWithNewData(databases, dbData, newData, removed) {
    const dbDataByCode = new Map(dbData.map(item => [item.kurulus_kodu, item]));

    // 1) Removed olanları sil
    for (let i = 0; i < removed.length; i++) {
        const item = removed[i];
        const existing = dbDataByCode.get(item.kurulus_kodu);

        if (existing && existing.docId) {
            await databases.deleteDocument(
                APPWRITE_DATABASE_ID,
                APPWRITE_COLLECTION_ID,
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
                APPWRITE_COLLECTION_ID,
                existing.docId,
                payload
            );
        } else {
            await databases.createDocument(
                APPWRITE_DATABASE_ID,
                APPWRITE_COLLECTION_ID,
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

async function sendReportMail({ html }) {
    await fetch(MAIL_FUNCTION_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            to: "alp.bayram@todeb.org.tr",
            subject: "WebWatcher Güncelleme Raporu",
            html
        })
    });
}

// =====================
//  ANA ÇALIŞTIRMA
// =====================

async function run(distillPayload) {
    const { databases } = createClient();

    const dbData = await getDbData(databases); // oldData

    const { meta, newData } = mapDistillToNewData(distillPayload); // newData

    const { added, removed } = compareKuruluslar(dbData, newData);
    const commonKuruluslar = getCommonKuruluslar(dbData, newData);
    const { degisenler3 } = kontrolEt(commonKuruluslar, dbData);

    // Şimdilik html = JSON dump, sadece deneme amaçlı
    const html = `
    <h1>${meta.name}</h1>
    <p><a href="${meta.uri}">${meta.uri}</a></p>
    <pre>${JSON.stringify(
        {
            added,
            removed,
            changed: degisenler3
        },
        null,
        2
    )}</pre>
  `;

    await sendReportMail({ html });

    await syncDbWithNewData(databases, dbData, newData, removed);

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
