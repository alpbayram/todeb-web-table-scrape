import { Client, Databases, ID, Query } from "node-appwrite";

// =====================
//  CONFIG (.env'den)
// =====================

const APPWRITE_ENDPOINT = process.env.APPWRITE_ENDPOINT;
const APPWRITE_PROJECT_ID = process.env.APPWRITE_PROJECT_ID;
const APPWRITE_API_KEY = process.env.APPWRITE_API_KEY;
const APPWRITE_DATABASE_ID = process.env.DATABASE_ID;

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
//  MAIL FUNCTION ÇAĞIRMA
// =====================

async function sendReportMail({ meta, added, removed, changed }) {
    await fetch(MAIL_FUNCTION_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            to: meta.to,
            subject: meta.subject || "Güncelleme Raporu",
            meta,
            added,
            removed,
            changed
        })
    });
}

// ====================================================
//  📌 WATCHERS (ID -> PARSER + DB + COMPARE + SYNC)
// ====================================================
// Yeni site geldikçe buraya yeni watcher ekleyeceksin.
// Motor hiçbir schema bilmez.
// ====================================================

const WATCHERS = {
    // ------------------------------------------------
    // TCMB Ödeme Kuruluşları Tablosu
    // ------------------------------------------------
    "tcmb_odeme_kuruluslari": {
        // distillPayload.text -> JSON string array
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const arr = JSON.parse(text);

            const newData = arr.map(item => ({
                kurulus_kodu: String(item.code).trim(),
                kurulus_adi: String(item.name).trim(),
                yetkiler: Array.isArray(item.rights) ? item.rights : []
            }));

            const trDate = ts
                ? new Date(ts).toLocaleString("tr-TR", {
                    timeZone: "Europe/Istanbul",
                    year: "numeric",
                    month: "2-digit",
                    day: "2-digit",
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit"
                })
                : null;

            return {
                meta: { id, name, uri, trDate, to, dbCollection },
                newData
            };
        },

        // ✅ TCMB'ye özel oldData okuma (pagination dahil)
        async getOldData(databases, meta) {
            const limit = 100;
            let offset = 0;
            let allDocs = [];
            let keepGoing = true;

            while (keepGoing) {
                const page = await databases.listDocuments(
                    APPWRITE_DATABASE_ID,
                    meta.dbCollection,
                    [Query.limit(limit), Query.offset(offset)]
                );

                allDocs = allDocs.concat(page.documents);

                if (page.documents.length < limit) {
                    keepGoing = false;
                } else {
                    offset = offset + limit;
                }
            }

            return allDocs.map(doc => ({
                docId: doc.$id,
                kurulus_kodu: doc.kurulus_kodu,
                kurulus_adi: doc.kurulus_adi,
                yetkiler: doc.yetkiler || []
            }));
        },

        // ✅ TCMB'ye özel compare
        compare(oldData, newData) {
            // ---- Added / Removed ----
            const oldCodes = new Set(oldData.map(i => i.kurulus_kodu));
            const newCodes = new Set(newData.map(i => i.kurulus_kodu));

            const added = newData.filter(i => !oldCodes.has(i.kurulus_kodu));
            const removed = oldData.filter(i => !newCodes.has(i.kurulus_kodu));

            // ---- Common: NEW DATA içinden (DB'de olan kodlar) ----
            const commonKuruluslar = newData.filter(i => oldCodes.has(i.kurulus_kodu));

            // ---- Changed Ad / Yetki ----
            const degisenlerName = [];
            const degisenlerRights = [];

            for (let i = 0; i < commonKuruluslar.length; i++) {
                const item = commonKuruluslar[i];
                const kod = item.kurulus_kodu;

                const oldItem = oldData.find(x => x.kurulus_kodu === kod);
                if (!oldItem) continue;

                if (item.kurulus_adi !== oldItem.kurulus_adi) {
                    degisenlerName.push(item);
                }

                const yeniYetkiler = item.yetkiler || [];
                const eskiYetkiler = oldItem.yetkiler || [];

                const yetkiDegistiMi =
                    eskiYetkiler.length !== yeniYetkiler.length ||
                    eskiYetkiler.some(y => !yeniYetkiler.includes(y)) ||
                    yeniYetkiler.some(y => !eskiYetkiler.includes(y));

                if (yetkiDegistiMi) {
                    degisenlerRights.push(item);
                }
            }

            // uniq NEW items
            const tumDegisenler = [...degisenlerName, ...degisenlerRights];
            const seen = new Set();
            const uniqNewItems = [];

            for (let i = 0; i < tumDegisenler.length; i++) {
                const it = tumDegisenler[i];
                if (!seen.has(it.kurulus_kodu)) {
                    seen.add(it.kurulus_kodu);
                    uniqNewItems.push(it);
                }
            }

            // changed: yeni + eski alanları aynı objede
            const changed = uniqNewItems.map(newItem => {
                const kod = newItem.kurulus_kodu;
                const oldItem = oldData.find(x => x.kurulus_kodu === kod) || {};

                return {
                    kurulus_kodu: kod,
                    kurulus_adi: newItem.kurulus_adi,
                    kurulus_adi_eski: oldItem.kurulus_adi ?? null,
                    yetkiler: newItem.yetkiler || [],
                    yetkiler_eski: oldItem.yetkiler || []
                };
            });

            return { added, removed, changed };
        },

        // ✅ TCMB'ye özel DB senkronu
        async syncDb(databases, oldData, newData, removed, meta) {
            const byCode = new Map(oldData.map(i => [i.kurulus_kodu, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = byCode.get(item.kurulus_kodu);
                if (existing?.docId) {
                    await databases.deleteDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollection,
                        existing.docId
                    );
                }
            }

            // newData upsert
            for (let i = 0; i < newData.length; i++) {
                const item = newData[i];
                const existing = byCode.get(item.kurulus_kodu);

                const payload = {
                    kurulus_kodu: item.kurulus_kodu,
                    kurulus_adi: item.kurulus_adi,
                    yetkiler: item.yetkiler
                };

                if (existing?.docId) {
                    await databases.updateDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollection,
                        existing.docId,
                        payload
                    );
                } else {
                    await databases.createDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollection,
                        ID.unique(),
                        payload
                    );
                }
            }
        }
    }
};

// =====================
//  ANA MOTOR
// =====================

async function run(distillPayload) {
    const watcher = WATCHERS[distillPayload.id];

    if (!watcher) {
        throw new Error(`Bu Distill ID için watcher tanımlı değil: ${distillPayload.id}`);
    }

    const { databases } = createClient();

    // 1) payload -> meta + newData
    const { meta, newData } = watcher.parseNewData(distillPayload);

    // 2) DB -> oldData (watcher bilir)
    const oldData = await watcher.getOldData(databases, meta);

    // 3) compare (watcher bilir)
    const { added, removed, changed } = watcher.compare(oldData, newData);

    // 4) mail gönder
    await sendReportMail({ meta, added, removed, changed });

    // 5) DB sync (watcher bilir)
    await watcher.syncDb(databases, oldData, newData, removed, meta);

    return { meta, added, removed, changed };
}

// =====================
//  APPWRITE FUNCTION HANDLER
// =====================

export default async ({ req, res, log, error }) => {
    try {
        const body =
            typeof req.body === "string" ? JSON.parse(req.body) : (req.body ?? {});

        const result = await run(body);

        return res.json({
            success: true,
            ...result
        });
    } catch (err) {
        if (error) error(err);
        else console.error(err);

        return res.json({
            success: false,
            error: err.message ?? String(err)
        });
    }
};
