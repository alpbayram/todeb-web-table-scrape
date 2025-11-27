import { Client, Databases, ID, Query } from "node-appwrite";
import fetch from "node-fetch";
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
//  RETRY + THROTTLE HELPERS
// =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withRetry(fn, { retries = 3, baseDelay = 250 } = {}) {
    let attempt = 0;

    while (true) {
        try {
            return await fn();
        } catch (e) {
            attempt++;
            const code = e?.code;

            // transient sayılacak hatalar
            const transient = code === 500 || code === 429 || code === 503 || !code;

            if (!transient || attempt > retries) throw e;

            const delay = baseDelay * attempt + Math.floor(Math.random() * 200);
            await sleep(delay);
        }
    }
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
            changed,
        }),
    });
}

import fetch from "node-fetch";

async function detectLanguage(text) {
    const apiKey = process.env.HF_TOKEN;

    const response = await fetch(
        "https://api-inference.huggingface.co/models/papluca/xlm-roberta-base-language-detection",
        {
            method: "POST",
            headers: {
                "Authorization": `Bearer ${apiKey}`,
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                inputs: text
            })
        }
    );

    const result = await response.json();

    // Hata / boş dönüş kontrolü
    if (!Array.isArray(result) || !result[0] || !Array.isArray(result[0])) {
        return null;
    }

    const top = result[0][0]; // en yüksek skor
    return top?.label || null;
}

// ====================================================
//  📌 WATCHERS (id -> parser + oldData + compare + sync)
// ====================================================

const WATCHERS = {
    // ------------------------------------------------
    // TCMB Ödeme Kuruluşları Tablosu
    // ------------------------------------------------
    tcmb_odeme_kuruluslari: {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const arr = JSON.parse(text);

            const newData = arr.map((item) => ({
                kurulus_kodu: String(item.code).trim(),
                kurulus_adi: String(item.name).trim(),
                yetkiler: Array.isArray(item.rights) ? item.rights : [],
            }));

            const trDate = ts
                ? new Date(ts).toLocaleString("tr-TR", {
                    timeZone: "Europe/Istanbul",
                    year: "numeric",
                    month: "2-digit",
                    day: "2-digit",
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                })
                : null;

            return {
                meta: { id, name, uri, trDate, to, dbCollection },
                newData,
            };
        },

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

                if (page.documents.length < limit) keepGoing = false;
                else offset += limit;
            }

            return allDocs.map((doc) => ({
                docId: doc.$id,
                kurulus_kodu: doc.kurulus_kodu,
                kurulus_adi: doc.kurulus_adi,
                yetkiler: doc.yetkiler || [],
            }));
        },

        compare(oldData, newData) {
            const oldCodes = new Set(oldData.map((i) => i.kurulus_kodu));
            const newCodes = new Set(newData.map((i) => i.kurulus_kodu));

            const added = newData.filter((i) => !oldCodes.has(i.kurulus_kodu));
            const removed = oldData.filter((i) => !newCodes.has(i.kurulus_kodu));

            const common = newData.filter((i) => oldCodes.has(i.kurulus_kodu));

            const degisenlerName = [];
            const degisenlerRights = [];

            for (let i = 0; i < common.length; i++) {
                const item = common[i];
                const kod = item.kurulus_kodu;

                const oldItem = oldData.find((x) => x.kurulus_kodu === kod);
                if (!oldItem) continue;

                if (item.kurulus_adi !== oldItem.kurulus_adi) {
                    degisenlerName.push(item);
                }

                const yeniYetkiler = item.yetkiler || [];
                const eskiYetkiler = oldItem.yetkiler || [];
                const yetkiDegistiMi =
                    eskiYetkiler.length !== yeniYetkiler.length ||
                    eskiYetkiler.some((y) => !yeniYetkiler.includes(y)) ||
                    yeniYetkiler.some((y) => !eskiYetkiler.includes(y));

                if (yetkiDegistiMi) degisenlerRights.push(item);
            }

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

            const changed = uniqNewItems.map((newItem) => {
                const kod = newItem.kurulus_kodu;
                const oldItem = oldData.find((x) => x.kurulus_kodu === kod) || {};
                return {
                    kurulus_kodu: kod,
                    kurulus_adi: newItem.kurulus_adi,
                    kurulus_adi_eski: oldItem.kurulus_adi ?? null,
                    yetkiler: newItem.yetkiler || [],
                    yetkiler_eski: oldItem.yetkiler || [],
                };
            });

            return { added, removed, changed };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const byCode = new Map(oldData.map((i) => [i.kurulus_kodu, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = byCode.get(item.kurulus_kodu);
                if (existing?.docId) {
                    await withRetry(() =>
                        databases.deleteDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollection,
                            existing.docId
                        )
                    );
                }
            }

            // newData içinde aynı kod varsa tekilleştir
            const uniqMap = new Map();
            for (const item of newData) {
                if (!item.kurulus_kodu) continue;
                uniqMap.set(item.kurulus_kodu, item);
            }
            const uniqNewData = Array.from(uniqMap.values());

            // upsert (retry + throttle)
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = byCode.get(item.kurulus_kodu);

                const payload = {
                    kurulus_kodu: item.kurulus_kodu,
                    kurulus_adi: item.kurulus_adi,
                    yetkiler: item.yetkiler || [],
                };

                try {
                    if (existing?.docId) {
                        await withRetry(() =>
                            databases.updateDocument(
                                APPWRITE_DATABASE_ID,
                                meta.dbCollection,
                                existing.docId,
                                payload
                            )
                        );
                    } else {
                        await withRetry(() =>
                            databases.createDocument(
                                APPWRITE_DATABASE_ID,
                                meta.dbCollection,
                                ID.unique(),
                                payload
                            )
                        );
                    }
                } catch (e) {
                    console.log("DB WRITE FAIL ITEM =>", item);
                    console.log("ERR message =>", e?.message);
                    console.log("ERR code =>", e?.code);
                    console.log("ERR type =>", e?.type);
                    console.log("ERR response =>", e?.response);
                }

                if ((i + 1) % 10 === 0) await sleep(150);
            }
        },
    },

    // ------------------------------------------------
    // Title-only Liste (TODEB/T.C.M.B. mevzuat duyuru vb.)
    // ------------------------------------------------
    tcmb_odeme_sistemleri_ile_ilgili_mevzuat: {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const arr = JSON.parse(text);

            const newData = arr
                .map((item) => ({
                    title: String(item.title).trim(),
                }))
                .filter((x) => x.title);

            const trDate = ts
                ? new Date(ts).toLocaleString("tr-TR", {
                    timeZone: "Europe/Istanbul",
                    year: "numeric",
                    month: "2-digit",
                    day: "2-digit",
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                })
                : null;

            return {
                meta: { id, name, uri, trDate, to, dbCollection },
                newData,
            };
        },

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

                if (page.documents.length < limit) keepGoing = false;
                else offset += limit;
            }

            return allDocs.map((doc) => ({
                docId: doc.$id,
                title: doc.title,
            }));
        },

        compare(oldData, newData) {
            const oldTitles = new Set(oldData.map((i) => i.title));
            const newTitles = new Set(newData.map((i) => i.title));

            const added = newData.filter((i) => !oldTitles.has(i.title));
            const removed = oldData.filter((i) => !newTitles.has(i.title));

            return { added, removed, changed: [] };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(oldData.map((i) => [i.title, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.title);
                if (existing?.docId) {
                    await withRetry(() =>
                        databases.deleteDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollection,
                            existing.docId
                        )
                    );
                }
            }

            // newData uniq
            const uniqTitles = new Map();
            for (const item of newData) {
                if (item.title) uniqTitles.set(item.title, item);
            }
            const uniqNewData = Array.from(uniqTitles.values());

            // sadece olmayanı create et (retry + throttle)
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = oldMap.get(item.title);

                if (!existing) {
                    try {
                        await withRetry(() =>
                            databases.createDocument(
                                APPWRITE_DATABASE_ID,
                                meta.dbCollection,
                                ID.unique(),
                                { title: item.title }
                            )
                        );
                    } catch (e) {
                        console.log("DB WRITE FAIL ITEM =>", item);
                        console.log("ERR message =>", e?.message);
                        console.log("ERR code =>", e?.code);
                        console.log("ERR type =>", e?.type);
                        console.log("ERR response =>", e?.response);
                    }
                }

                if ((i + 1) % 10 === 0) await sleep(150);
            }
        },
    },
    "tcmb_odeme_kuruluslari_paragraf": {
        // Distill payload -> meta + newData
        // Distill text: HTML string (tek parça)
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const textHtml = String(text || "").trim();

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
                // tek kayıt gibi davranacağız
                newData: [{ textHtml }]
            };
        },

        // DB'den eski textHtml'i çek
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

                if (page.documents.length < limit) keepGoing = false;
                else offset += limit;
            }

            // collection tek kayıt tutacak ama yanlışlıkla çok kayıt olursa
            // ilkini "current" sayıyoruz
            return allDocs.map(doc => ({
                docId: doc.$id,
                textHtml: doc.textHtml || ""
            }));
        },

        // Compare: sadece string karşılaştır
        compare(oldData, newData) {
            const oldItem = oldData[0]; // mevcut tek kayıt
            const newItem = newData[0];

            // DB bomboşsa: bu bir "added"
            if (!oldItem) {
                return {
                    added: [newItem],
                    removed: [],
                    changed: []
                };
            }

            // değişim varsa "changed"
            const changed =
                String(oldItem.textHtml || "").trim() !== String(newItem.textHtml || "").trim()
                    ? [
                        {
                            textHtml: newItem.textHtml,
                            textHtml_eski: oldItem.textHtml
                        }
                    ]
                    : [];

            return {
                added: [],
                removed: [],
                changed
            };
        },

        // DB senkronu: tek kayıt upsert
        async syncDb(databases, oldData, newData, removed, meta) {
            const oldItem = oldData[0];
            const newItem = newData[0];

            const payload = { textHtml: newItem.textHtml };

            if (oldItem?.docId) {
                await databases.updateDocument(
                    APPWRITE_DATABASE_ID,
                    meta.dbCollection,
                    oldItem.docId,
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
    },
    "masak_mevzuat": {
        async parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const arr = JSON.parse(text); // [{ title: "..." }, ...]

            const newData = [];

            for (const item of arr) {
                const rawTitle = String(item.title || "").trim();
                if (!rawTitle) continue;

                const lang = await detectLanguage(rawTitle);

                // sadece Türkçe olanlar DB'ye alınır
                if (lang === "tr") {
                    newData.push({ title: rawTitle });
                }
            }

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

                if (page.documents.length < limit) keepGoing = false;
                else offset += limit;
            }

            return allDocs.map(doc => ({
                docId: doc.$id,
                title: doc.title
            }));
        },

        compare(oldData, newData) {
            const oldSet = new Set(oldData.map(i => i.title));
            const newSet = new Set(newData.map(i => i.title));

            const added = newData.filter(i => !oldSet.has(i.title));
            const removed = oldData.filter(i => !newSet.has(i.title));

            return {
                added,
                removed,
                changed: [] // title-only watcher, değişim yok
            };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const map = new Map(oldData.map(i => [i.title, i]));

            // removed sil
            for (const item of removed) {
                const existing = map.get(item.title);
                if (existing?.docId) {
                    await databases.deleteDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollection,
                        existing.docId
                    );
                }
            }

            // added ekle
            for (const item of newData) {
                const existing = map.get(item.title);
                if (!existing) {
                    await databases.createDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollection,
                        ID.unique(),
                        { title: item.title }
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
        throw new Error(
            `Bu Distill ID için watcher tanımlı değil: ${distillPayload.id}`
        );
    }

    const { databases } = createClient();

    // 1) payload -> meta + newData
    const { meta, newData } = watcher.parseNewData(distillPayload);

    // 2) oldData
    const oldData = await watcher.getOldData(databases, meta);

    // 3) compare
    const { added, removed, changed } = watcher.compare(oldData, newData);

    // 4) mail
    await sendReportMail({ meta, added, removed, changed });

    // 5) sync
    await watcher.syncDb(databases, oldData, newData, removed, meta);

    return { meta, added, removed, changed };
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
            ...result,
        });
    } catch (err) {
        if (error) error(err);
        else console.error(err);

        return res.json({
            success: false,
            error: err.message ?? String(err),
        });
    }
};
