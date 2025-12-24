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
async function enqueueToPool(databases, meta, rawBody) {
    const poolCollection = rawBody.dbCollectionPool || meta.dbCollectionPool;

    if (!poolCollection) {
        throw new Error("mode=pool ama dbCollectionPool yok (payload/db).");
    }

    // ✅ Pool’a minimal payload yaz
    const poolPayloadObj = buildPoolPayload(rawBody);

    await databases.createDocument(
        APPWRITE_DATABASE_ID,
        poolCollection,
        ID.unique(),
        {
            payload: JSON.stringify(poolPayloadObj),

            // sourceId: istersen kalsın (opsiyonel)
            sourceId: meta?.id || rawBody?.id || null
        }
    );
}

function buildPoolPayload({ meta, added, removed /* changed */ }) {
    return {
        meta: {
            name: meta?.name || "",
            uri: meta?.uri || ""
            // date yok, id yok, mode yok, dbCollection yok, to yok
        },
        added: added || [],
        removed: removed || []
        // changed pool'da istemiyorsun -> eklemiyoruz
    };
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
    "tcmb_odeme_kuruluslari_table_paragraf": {
        // --------------------
        //  parseNewData
        // --------------------
        parseNewData(distillPayload) {
            const {
                id,
                name,
                uri,
                text,
                ts,
                to,
                dbCollectionTable,
                dbCollectionHtml
            } = distillPayload;

            const root = JSON.parse(text || "{}");
            const tableArr = Array.isArray(root.table) ? root.table : [];
            const htmlArr = Array.isArray(root.html) ? root.html : [];
            const htmlRaw = htmlArr[0] || root.html || "";

            // TABLO: önceki TCMB watcher ile aynı mantık
            const tableNewData = tableArr.map(item => ({
                kurulus_kodu: String(item.code ?? "").trim(),
                kurulus_adi: String(item.name ?? "").trim(),
                yetkiler: Array.isArray(item.rights) ? item.rights : []
            }));

            // HTML: tek satır, textHtml alanında tutacağız
            const htmlNewData = htmlRaw
                ? [
                    {
                        textHtml: String(htmlRaw)
                    }
                ]
                : [];

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
                meta: {
                    id,
                    name,
                    uri,
                    trDate,
                    to,
                    dbCollectionTable,
                    dbCollectionHtml
                },
                // iki ayrı dataset beraber dönüyor
                newData: {
                    table: tableNewData,
                    html: htmlNewData
                }
            };
        },

        // --------------------
        //  getOldData
        // --------------------
        async getOldData(databases, meta) {
            const limit = 100;

            async function loadAll(collectionId, mapFn) {
                if (!collectionId) return [];

                let offset = 0;
                let allDocs = [];
                let keepGoing = true;

                while (keepGoing) {
                    const page = await databases.listDocuments(
                        APPWRITE_DATABASE_ID,
                        collectionId,
                        [Query.limit(limit), Query.offset(offset)]
                    );

                    allDocs = allDocs.concat(page.documents);

                    if (page.documents.length < limit) {
                        keepGoing = false;
                    } else {
                        offset += limit;
                    }
                }

                return allDocs.map(mapFn);
            }

            // TABLO: kurulus_kodu / kurulus_adi / yetkiler
            const tableOld = await loadAll(meta.dbCollectionTable, doc => ({
                docId: doc.$id,
                kurulus_kodu: doc.kurulus_kodu,
                kurulus_adi: doc.kurulus_adi,
                yetkiler: doc.yetkiler || []
            }));

            // HTML: tek kayıt, textHtml alanında
            const htmlOld = await loadAll(meta.dbCollectionHtml, doc => ({
                docId: doc.$id,
                textHtml: doc.textHtml || ""
            }));

            return {
                table: tableOld,
                html: htmlOld
            };
        },

        // --------------------
        //  compare
        // --------------------
        compare(oldData, newData) {
            const oldTable = oldData.table || [];
            const newTable = newData.table || [];

            const oldHtml = oldData.html || [];
            const newHtml = newData.html || [];

            // ==== TABLO KARŞILAŞTIRMA (eski TCMB mantığı) ====
            const oldCodes = new Set(oldTable.map(i => i.kurulus_kodu));
            const newCodes = new Set(newTable.map(i => i.kurulus_kodu));

            const tableAdded = newTable.filter(i => !oldCodes.has(i.kurulus_kodu));
            const tableRemoved = oldTable.filter(i => !newCodes.has(i.kurulus_kodu));

            const common = newTable.filter(i => oldCodes.has(i.kurulus_kodu));

            const degisenName = [];
            const degisenRights = [];

            for (let i = 0; i < common.length; i++) {
                const item = common[i];
                const kod = item.kurulus_kodu;
                const oldItem = oldTable.find(x => x.kurulus_kodu === kod);
                if (!oldItem) continue;

                if (item.kurulus_adi !== oldItem.kurulus_adi) {
                    degisenName.push(item);
                }

                const yeniYet = item.yetkiler || [];
                const eskiYet = oldItem.yetkiler || [];

                const yetkiDegisti =
                    eskiYet.length !== yeniYet.length ||
                    eskiYet.some(y => !yeniYet.includes(y)) ||
                    yeniYet.some(y => !eskiYet.includes(y));

                if (yetkiDegisti) {
                    degisenRights.push(item);
                }
            }

            const tumDegisen = [...degisenName, ...degisenRights];
            const seen = new Set();
            const uniqNew = [];

            for (let i = 0; i < tumDegisen.length; i++) {
                const it = tumDegisen[i];
                if (!seen.has(it.kurulus_kodu)) {
                    seen.add(it.kurulus_kodu);
                    uniqNew.push(it);
                }
            }

            const tableChanged = uniqNew.map(newItem => {
                const kod = newItem.kurulus_kodu;
                const oldItem = oldTable.find(x => x.kurulus_kodu === kod) || {};

                return {
                    kurulus_kodu: kod,
                    kurulus_adi: newItem.kurulus_adi,
                    kurulus_adi_eski: oldItem.kurulus_adi ?? null,
                    yetkiler: newItem.yetkiler || [],
                    yetkiler_eski: oldItem.yetkiler || []
                };
            });

            // ==== HTML KARŞILAŞTIRMA (tek kayıt) ====
            let htmlAdded = [];
            let htmlRemoved = [];
            let htmlChanged = [];

            const oldHtmlItem = oldHtml[0];
            const newHtmlItem = newHtml[0];

            if (!oldHtmlItem && newHtmlItem) {
                // databasede yoktu → eklendi
                htmlAdded = [newHtmlItem];
            } else if (oldHtmlItem && !newHtmlItem) {
                // databasede vardı, sayfadan kalktı
                htmlRemoved = [oldHtmlItem];
            } else if (oldHtmlItem && newHtmlItem) {
                if ((oldHtmlItem.textHtml || "") !== (newHtmlItem.textHtml || "")) {
                    htmlChanged = [
                        {
                            textHtml_eski: oldHtmlItem.textHtml || "",
                            textHtml: newHtmlItem.textHtml || ""
                        }
                    ];
                }
            }

            // sendReportMail için birleşik obje
            return {
                added: {
                    table: tableAdded,
                    html: htmlAdded
                },
                removed: {
                    table: tableRemoved,
                    html: htmlRemoved
                },
                changed: {
                    table: tableChanged,
                    html: htmlChanged
                }
            };
        },

        // --------------------
        //  syncDb
        // --------------------
        async syncDb(databases, oldData, newData, removed, meta) {
            const oldTable = oldData.table || [];
            const newTable = newData.table || [];

            const oldHtml = oldData.html || [];
            const newHtml = newData.html || [];

            const removedTable = (removed && removed.table) || [];
            const removedHtml = (removed && removed.html) || [];

            // === TABLO SENKRONU (eski TCMB syncDb ile aynı mantık) ===
            if (meta.dbCollectionTable) {
                const byCode = new Map(
                    oldTable.map(i => [i.kurulus_kodu, i])
                );

                // removed sil
                for (let i = 0; i < removedTable.length; i++) {
                    const item = removedTable[i];
                    const existing = byCode.get(item.kurulus_kodu);
                    if (existing?.docId) {
                        await databases.deleteDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollectionTable,
                            existing.docId
                        );
                    }
                }

                // upsert
                for (let i = 0; i < newTable.length; i++) {
                    const item = newTable[i];
                    const existing = byCode.get(item.kurulus_kodu);

                    const payload = {
                        kurulus_kodu: item.kurulus_kodu,
                        kurulus_adi: item.kurulus_adi,
                        yetkiler: item.yetkiler
                    };

                    if (existing?.docId) {
                        await databases.updateDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollectionTable,
                            existing.docId,
                            payload
                        );
                    } else {
                        await databases.createDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollectionTable,
                            ID.unique(),
                            payload
                        );
                    }
                }
            }

            // === HTML SENKRONU (tek kayıt, textHtml alanı) ===
            if (meta.dbCollectionHtml) {
                const oldItem = oldHtml[0] || null;
                const newItem = newHtml[0] || null;

                // removed varsa hepsini sil (biz zaten max 1 kayıt bekliyoruz)
                if (removedHtml.length && oldItem?.docId) {
                    await databases.deleteDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollectionHtml,
                        oldItem.docId
                    );
                }

                // yeni html varsa create/update
                if (newItem) {
                    const payload = { textHtml: newItem.textHtml || "" };

                    if (oldItem?.docId) {
                        await databases.updateDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollectionHtml,
                            oldItem.docId,
                            payload
                        );
                    } else {
                        await databases.createDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollectionHtml,
                            ID.unique(),
                            payload
                        );
                    }
                }
            }
        }
    },

    // ------------------------------------------------
    // Title-only Liste (TODEB/T.C.M.B. mevzuat duyuru vb.)
    // ------------------------------------------------
    tcmb_odeme_sistemleri_ile_ilgili_mevzuat: {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            // Distill JS şunu dönüyor:
            // [ { id: "...uuid...", title: "...", href: "https://..." }, ... ]
            const arr = JSON.parse(text);

            const newData = arr
                .map(item => ({
                    mevzuat_id: String(item.id || "").trim(),
                    title: String(item.title || "").trim(),
                    href: item.href || null  // mail için payload’da kalsın
                }))
                // hem id hem title dolu olmayanları at
                .filter(x => x.mevzuat_id && x.title);

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

            // DB şeması:
            // mevzuat_id (string, uniq), title (string)
            return allDocs.map(doc => ({
                docId: doc.$id,
                mevzuat_id: doc.mevzuat_id,
                title: doc.title
            }));
        },

        compare(oldData, newData) {
            // 🔑 Artık key = mevzuat_id
            const oldIds = new Set(oldData.map(i => i.mevzuat_id));
            const newIds = new Set(newData.map(i => i.mevzuat_id));

            const added = newData.filter(i => !oldIds.has(i.mevzuat_id));
            const removed = oldData.filter(i => !newIds.has(i.mevzuat_id));

            // title değişse bile (id aynı olduğu sürece) changed saymıyoruz,
            // istersen ilerde burada ayrı changed hesaplayabiliriz.
            return { added, removed, changed: [] };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            // Eski kayıtları mevzuat_id üzerinden map'le
            const oldMap = new Map(oldData.map(i => [i.mevzuat_id, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.mevzuat_id);
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

            // newData içinde duplicate mevzuat_id varsa uniq'le
            const uniqById = new Map();
            for (const item of newData) {
                if (item.mevzuat_id) uniqById.set(item.mevzuat_id, item);
            }
            const uniqNewData = Array.from(uniqById.values());

            // sadece DB'de olmayanları create et (mevzuat_id bazlı)
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = oldMap.get(item.mevzuat_id);

                if (!existing) {
                    const payload = {
                        mevzuat_id: item.mevzuat_id,
                        title: item.title
                    };

                    try {
                        await withRetry(() =>
                            databases.createDocument(
                                APPWRITE_DATABASE_ID,
                                meta.dbCollection,
                                ID.unique(),
                                payload
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

                // rate limit'e takılmamak için
                if ((i + 1) % 10 === 0) await sleep(150);
            }
        }
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
        // DistillPayload.text -> JSON string array: [{ title, href }, ...]
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const arr = JSON.parse(text);

            // newData: href + title
            const newDataRaw = (Array.isArray(arr) ? arr : [])
                .map(item => ({
                    title: String(item?.title || "").trim(),
                    href: String(item?.href || "").trim()
                }))
                .filter(x => x.href); // href boşsa at

            // href uniq
            const uniq = new Map();
            for (const it of newDataRaw) {
                if (!uniq.has(it.href)) uniq.set(it.href, it);
            }
            const newData = Array.from(uniq.values());

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
                title: doc.title || "",
                href: doc.href || ""
            }));
        },

        compare(oldData, newData) {
            const oldHrefs = new Set(oldData.map(i => i.href).filter(Boolean));
            const newHrefs = new Set(newData.map(i => i.href).filter(Boolean));

            const added = newData.filter(i => !oldHrefs.has(i.href));
            const removed = oldData.filter(i => !newHrefs.has(i.href));

            return { added, removed, changed: [] };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(oldData.map(i => [i.href, i])); // href -> old row

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.href);

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

            // create (sadece yoksa)
            for (let i = 0; i < newData.length; i++) {
                const item = newData[i];
                const existing = oldMap.get(item.href);

                if (!existing) {
                    await withRetry(() =>
                        databases.createDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollection,
                            ID.unique(),
                            {
                                title: item.title,
                                href: item.href
                            }
                        )
                    );
                }

                if ((i + 1) % 10 === 0) await sleep(150);
            }
        }
    },

    "duyurular": {
        // Distill payload → { meta, newData }
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            // text: JSON string
            // Örnek beklenen format:
            // {
            //   "resultContainer": {
            //     "content": [
            //       { "id": "123", "title": "..." },
            //       { "id": "124", "title": "..." }
            //     ]
            //   }
            // }
            const parsed = JSON.parse(text);

            let arr = [];

            if (Array.isArray(parsed)) {
                // İleride direkt array gönderirsen:
                // [ { id, title }, ... ]
                arr = parsed;
            } else if (
                parsed &&
                parsed.resultContainer &&
                Array.isArray(parsed.resultContainer.content)
            ) {
                arr = parsed.resultContainer.content;
            } else {
                throw new Error("Beklenmeyen JSON formatı (duyurular watcher)");
            }

            const newData = arr
                .map(item => ({
                    // 🔑 Artık duyuru_id anahtarımız
                    duyuru_id: String(item.id ?? "").trim(),
                    title: String(item.title ?? "").trim(),
                    slug: item.slug ? String(item.slug).trim() : null
                }))
                .filter(x => x.duyuru_id && x.title); // hem id hem title boş değilse

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

        // DB → oldData (pagination’lı)
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
                    offset += limit;
                }
            }

            // Collection’ında şu alanların olduğundan emin ol:
            // - duyuru_id (string)
            // - title (string)
            // - (opsiyonel) href
            return allDocs.map(doc => ({
                docId: doc.$id,
                duyuru_id: doc.duyuru_id,
                title: doc.title,
                href: doc.href || null
            }));
        },

        // added / removed id'ye göre
        compare(oldData, newData) {
            const oldIds = new Set(oldData.map(i => i.duyuru_id));
            const newIds = new Set(newData.map(i => i.duyuru_id));

            const added = newData.filter(i => !oldIds.has(i.duyuru_id));
            const removed = oldData.filter(i => !newIds.has(i.duyuru_id));

            // Duyurularda title değişimini "changed" olarak track etmiyoruz.
            // Aynı id için title değişirse, syncDb sırasında DB’deki title güncellenecek.
            const changed = [];

            return { added, removed, changed };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            // Map’i id üzerinden kuruyoruz
            const oldMap = new Map(oldData.map(i => [i.duyuru_id, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.duyuru_id);

                if (existing?.docId) {
                    await databases.deleteDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollection,
                        existing.docId
                    );
                }
            }

            // newData upsert (id aynıysa update, yoksa create)
            for (let i = 0; i < newData.length; i++) {
                const item = newData[i];
                const existing = oldMap.get(item.duyuru_id);

                const payload = {
                    duyuru_id: item.duyuru_id,
                    title: item.title
                };

                if (item.href) {
                    payload.href = item.href;
                }

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
    },

    "vergi_mevzuati": {
        // Distill payload -> { meta, newData }
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const parsed = JSON.parse(text || "{}");

            const results = Array.isArray(parsed?.resultContainer?.results)
                ? parsed.resultContainer.results
                : [];

            function buildMevzuatId(item) {
                const url = item.url || "";
                const match = url.match(/\/(\d+)$/);  // sondaki sayı
                if (!match) return null;

                const urlId = match[1];
                const entityType = String(item.entityType || "GENERIC").toUpperCase();
                return `${entityType}_${urlId}`;
            }

            const newDataRaw = results.map(item => {
                const mevzuat_id = buildMevzuatId(item);
                const title = String(item.text || "").trim();

                // URL zaten tam geliyorsa direkt kullanıyoruz
                const rawUrl = item.url || "";
                const href = rawUrl
                    ? (rawUrl.startsWith("http")
                        ? rawUrl
                        : `https://gib.gov.tr${rawUrl}`)
                    : null;

                return {
                    mevzuat_id,
                    title,
                    href
                };
            });

            // id veya title olmayanları at
            const newData = newDataRaw.filter(
                x => x.mevzuat_id && x.title
            );

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

                if (page.documents.length < limit) {
                    keepGoing = false;
                } else {
                    offset += limit;
                }
            }

            return allDocs.map(doc => ({
                docId: doc.$id,
                mevzuat_id: doc.mevzuat_id,
                title: doc.title
                // href DB'de yok, bilinçli olarak almıyoruz
            }));
        },

        compare(oldData, newData) {
            const oldIds = new Set(oldData.map(i => i.mevzuat_id));
            const newIds = new Set(newData.map(i => i.mevzuat_id));

            const added = newData.filter(i => !oldIds.has(i.mevzuat_id));
            const removed = oldData.filter(i => !newIds.has(i.mevzuat_id));

            // Şimdilik changed takibi yok
            const changed = [];

            return { added, removed, changed };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(oldData.map(i => [i.mevzuat_id, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.mevzuat_id);

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

            // newData uniq (mevzuat_id bazlı)
            const uniqMap = new Map();
            for (const item of newData) {
                if (item.mevzuat_id) {
                    uniqMap.set(item.mevzuat_id, {
                        mevzuat_id: item.mevzuat_id,
                        title: item.title
                        // href DB'ye gitmiyor
                    });
                }
            }
            const uniqNewData = Array.from(uniqMap.values());

            // upsert
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = oldMap.get(item.mevzuat_id);

                const payload = {
                    mevzuat_id: item.mevzuat_id,
                    title: item.title
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

                if ((i + 1) % 10 === 0) {
                    await sleep(150);
                }
            }
        }
    },
    // ------------------------------------------------
    // GİB - Taslaklar
    // Örnek JSON:
    // {
    //   "resultContainer": {
    //     "results": [
    //       { "id": 13, "text": "..." }
    //     ]
    //   }
    // }
    // DB sütunları: mevzuat_id, title
    // mevzuat_id = String(id)
    // ------------------------------------------------
    "gib_taslaklar": {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const parsed = JSON.parse(text);

            let arr = [];
            if (Array.isArray(parsed)) {
                arr = parsed;
            } else if (
                parsed &&
                parsed.resultContainer &&
                Array.isArray(parsed.resultContainer.content)
            ) {
                arr = parsed.resultContainer.content;
            } else {
                throw new Error("Beklenmeyen JSON formatı (gib_taslaklar)");
            }

            const BASE_URL = "https://gib.gov.tr";

            const newData = arr
                .map((item) => {
                    const rawId = item.id ?? item.ID ?? null;
                    const mevzuat_id = rawId != null ? String(rawId).trim() : "";

                    const title = String(item.text || item.title || "").trim();

                    if (!mevzuat_id || !title) return null;

                    const href = `${BASE_URL}/mevzuat/taslak/${mevzuat_id}`;

                    return {
                        mevzuat_id,
                        title,
                        href // DB'ye yazmayacağız ama mail function’da kullanabiliriz
                    };
                })
                .filter(Boolean); // null'ları at

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

            // DB: mevzuat_id, title
            return allDocs.map((doc) => ({
                docId: doc.$id,
                mevzuat_id: doc.mevzuat_id,
                title: doc.title
            }));
        },

        compare(oldData, newData) {
            const oldIds = new Set(oldData.map((i) => i.mevzuat_id));
            const newIds = new Set(newData.map((i) => i.mevzuat_id));

            const added = newData.filter((i) => !oldIds.has(i.mevzuat_id));
            const removed = oldData.filter((i) => !newIds.has(i.mevzuat_id));

            // İstersen title değişimini de changed olarak takip edelim
            const changed = [];

            for (let i = 0; i < newData.length; i++) {
                const item = newData[i];
                const oldItem = oldData.find(
                    (o) => o.mevzuat_id === item.mevzuat_id
                );
                if (!oldItem) continue;

                if ((oldItem.title || "") !== (item.title || "")) {
                    changed.push({
                        mevzuat_id: item.mevzuat_id,
                        title: item.title,
                        title_eski: oldItem.title || null,
                        href: item.href || null
                    });
                }
            }

            return { added, removed, changed };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(
                oldData.map((i) => [i.mevzuat_id, i])
            );

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.mevzuat_id);
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

            // newData uniq (mevzuat_id bazlı)
            const uniqMap = new Map();
            for (const item of newData) {
                if (item.mevzuat_id) {
                    uniqMap.set(item.mevzuat_id, item);
                }
            }
            const uniqNewData = Array.from(uniqMap.values());

            // upsert
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = oldMap.get(item.mevzuat_id);

                const payload = {
                    mevzuat_id: item.mevzuat_id,
                    title: item.title
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

                if ((i + 1) % 10 === 0) {
                    await sleep(150);
                }
            }
        }
    },
    // WATCHERS içine ekle:
    //  "masak_basin_duyuru": { ... }

    "masak_basin_duyuru": {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            // text: JSON string, örnek:
            // [
            //   { id: 5543, slug: "...", title: { rendered: "..." } },
            //   ...
            // ]
            const arr = JSON.parse(text);

            if (!Array.isArray(arr)) {
                throw new Error("Beklenmeyen JSON formatı (masak_basin_duyuru)");
            }

            const BASE_URL = "https://masak.hmb.gov.tr/duyuru/";

            const newData = arr
                .map(item => {
                    const duyuruId = String(item.id ?? "").trim();
                    const slug = item.slug || "";
                    const href = slug ? `${BASE_URL}${slug}/` : null;

                    // title.rendered HTML entity / tag içerebilir, direkt kullanıyoruz
                    const rawTitle =
                        (item.title && item.title.rendered) ||
                        item.title ||
                        "";

                    const title = String(rawTitle).trim();

                    return {
                        duyuru_id: duyuruId,
                        title,
                        slug,
                        href
                    };
                })
                .filter(x => x.duyuru_id && x.title); // id veya title boşsa alma

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

                if (page.documents.length < limit) {
                    keepGoing = false;
                } else {
                    offset += limit;
                }
            }

            // DB şeman:
            //  masak_basin_duyuru: { duyuru_id, title }
            return allDocs.map(doc => ({
                docId: doc.$id,
                duyuru_id: doc.duyuru_id,
                title: doc.title
            }));
        },

        compare(oldData, newData) {
            // uniq key: duyuru_id
            const oldIds = new Set(oldData.map(i => i.duyuru_id));
            const newIds = new Set(newData.map(i => i.duyuru_id));

            const added = newData.filter(i => !oldIds.has(i.duyuru_id));
            const removed = oldData.filter(i => !newIds.has(i.duyuru_id));

            // klasik "duyuru" mantığı: changed yok
            const changed = [];

            return { added, removed, changed };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(oldData.map(i => [i.duyuru_id, i]));

            // --- removed sil ---
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.duyuru_id);

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

            // --- newData uniq (aynı duyuru_id iki kere geldiyse sonuncuyu al) ---
            const uniqMap = new Map();
            for (const item of newData) {
                if (item.duyuru_id) {
                    uniqMap.set(item.duyuru_id, item);
                }
            }
            const uniqNewData = Array.from(uniqMap.values());

            // --- sadece DB'de olmayanları create et (title doldur) ---
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = oldMap.get(item.duyuru_id);

                if (!existing) {
                    const payload = {
                        duyuru_id: item.duyuru_id,
                        title: item.title
                    };

                    try {
                        await withRetry(() =>
                            databases.createDocument(
                                APPWRITE_DATABASE_ID,
                                meta.dbCollection,
                                ID.unique(),
                                payload
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

                // Appwrite rate limit'e nazik davranalım
                if ((i + 1) % 10 === 0) {
                    await sleep(150);
                }
            }
        }
    },

    "gib_kkdf": {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const parsed = JSON.parse(text);
            const arr = parsed?.resultContainer?.content || [];

            const baseUrl = "https://gib.gov.tr/mevzuat/kkdf";

            const newData = arr
                .map(item => {
                    const title = String(item.title || "").trim();
                    const mevzuatId = String(item.id || "").trim();

                    if (!title || !mevzuatId) return null;

                    const href = item.link
                        ? item.link.trim()
                        : `${baseUrl}/${mevzuatId}`;

                    return {
                        title,
                        mevzuat_id: mevzuatId,
                        href
                    };
                })
                .filter(Boolean);

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
            let list = [];
            let keep = true;

            while (keep) {
                const page = await databases.listDocuments(
                    APPWRITE_DATABASE_ID,
                    meta.dbCollection,
                    [Query.limit(limit), Query.offset(offset)]
                );

                list = list.concat(page.documents);

                if (page.documents.length < limit) keep = false;
                else offset += limit;
            }

            return list.map(doc => ({
                docId: doc.$id,
                mevzuat_id: doc.mevzuat_id,
                title: doc.title
            }));
        },

        compare(oldData, newData) {
            const oldIds = new Set(oldData.map(i => i.mevzuat_id));
            const newIds = new Set(newData.map(i => i.mevzuat_id));

            const added = newData.filter(i => !oldIds.has(i.mevzuat_id));
            const removed = oldData.filter(i => !newIds.has(i.mevzuat_id));

            return { added, removed, changed: [] };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(oldData.map(i => [i.mevzuat_id, i]));

            // removed sil
            for (const item of removed) {
                const existing = oldMap.get(item.mevzuat_id);
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

            // uniq new data
            const uniqMap = new Map();
            for (const item of newData) {
                uniqMap.set(item.mevzuat_id, item);
            }
            const uniqNew = [...uniqMap.values()];

            // create (update yok çünkü changed takip etmiyoruz)
            for (let i = 0; i < uniqNew.length; i++) {
                const item = uniqNew[i];
                if (!oldMap.has(item.mevzuat_id)) {
                    await withRetry(() =>
                        databases.createDocument(
                            APPWRITE_DATABASE_ID,
                            meta.dbCollection,
                            ID.unique(),
                            {
                                mevzuat_id: item.mevzuat_id,
                                title: item.title
                            }
                        )
                    );
                }

                if ((i + 1) % 10 === 0) await sleep(150);
            }
        }
    },

    "gib_uluslararasi_mevzuat": {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const parsed = JSON.parse(text);

            let arr = [];

            // Beklenen format: { resultContainer: { content: [...] } }
            if (
                parsed &&
                parsed.resultContainer &&
                Array.isArray(parsed.resultContainer.content)
            ) {
                arr = parsed.resultContainer.content;
            } else if (Array.isArray(parsed)) {
                // İleride direkt dizi dönerse de patlamasın
                arr = parsed;
            } else {
                throw new Error("Beklenmeyen JSON formatı (gib_uluslararasi_mevzuat)");
            }

            const BASE_URL = "https://gib.gov.tr";

            const newData = arr
                .map((item) => {
                    const rawId = item.id;
                    const rawTurId = item.turId;
                    const title = String(item.title || "").trim();

                    if (rawId == null || rawTurId == null) return null;

                    const turId = String(rawTurId).trim();
                    const docId = String(rawId).trim();

                    const mevzuat_id = `${turId}_${docId}`; // uniq key

                    const href = `${BASE_URL}/mevzuat/tur/${turId}/anlasma/${docId}`;

                    return {
                        mevzuat_id,
                        title,
                        href
                    };
                })
                .filter((x) => x && x.mevzuat_id && x.title);

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

            // DB sütunları: mevzuat_id, title
            return allDocs.map((doc) => ({
                docId: doc.$id,
                mevzuat_id: doc.mevzuat_id,
                title: doc.title
            }));
        },

        compare(oldData, newData) {
            // Artık title değil mevzuat_id üzerinden kıyaslıyoruz
            const oldIds = new Set(oldData.map((i) => i.mevzuat_id));
            const newIds = new Set(newData.map((i) => i.mevzuat_id));

            const added = newData.filter((i) => !oldIds.has(i.mevzuat_id));
            const removed = oldData.filter((i) => !newIds.has(i.mevzuat_id));

            // İstersek burada title değişimini de track edebiliriz.
            // Şimdilik gerek yok dedik -> changed boş.
            const changed = [];

            return { added, removed, changed };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(oldData.map((i) => [i.mevzuat_id, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.mevzuat_id);
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

            // newData uniq (mevzuat_id bazında)
            const uniqMap = new Map();
            for (const item of newData) {
                if (item.mevzuat_id) uniqMap.set(item.mevzuat_id, item);
            }
            const uniqNewData = Array.from(uniqMap.values());

            // upsert: varsa update (title güncelle), yoksa create
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = oldMap.get(item.mevzuat_id);

                const payload = {
                    mevzuat_id: item.mevzuat_id,
                    title: item.title
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

                // Çok yazıyorsak hafif fren
                if ((i + 1) % 10 === 0) {
                    await sleep(150);
                }
            }
        }
    },
    "bddk_mevzuat_mode_pool_test": {
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, mode, dbCollection, dbCollectionPool } = distillPayload;

            const arr = JSON.parse(text); // [{subfolder,title,href,indir,ek}, ...]

            const newData = (Array.isArray(arr) ? arr : [])
                .map(item => ({
                    subfolder: String(item.subfolder || "").trim(),
                    title: String(item.title || "").trim(),
                    href: String(item.href || "").trim(),
                    indir: item.indir ?? null,
                    ek: item.ek ?? null
                }))
                .filter(x => x.href && x.title); // href/title boşsa at

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
                meta: {
                    id,
                    name,
                    uri,
                    trDate,
                    to,
                    mode: mode || "direct",
                    dbCollection,
                    dbCollectionPool
                },
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
                subfolder: doc.subfolder || "",
                title: doc.title || "",
                href: doc.href || "",
                indir: doc.indir ?? null,
                ek: doc.ek ?? null
            }));
        },

        compare(oldData, newData) {
            const oldKeys = new Set(oldData.map(i => i.href));
            const newKeys = new Set(newData.map(i => i.href));

            const added = newData.filter(i => !oldKeys.has(i.href));
            const removed = oldData.filter(i => !newKeys.has(i.href));

            return { added, removed, changed: [] };
        },

        async syncDb(databases, oldData, newData, removed, meta) {
            const byHref = new Map(oldData.map(i => [i.href, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = byHref.get(item.href);
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

            // newData uniq (href)
            const uniq = new Map();
            for (const item of newData) {
                if (item.href) uniq.set(item.href, item);
            }
            const uniqNewData = Array.from(uniq.values());

            // upsert (href üzerinden)
            for (let i = 0; i < uniqNewData.length; i++) {
                const item = uniqNewData[i];
                const existing = byHref.get(item.href);

                const payload = {
                    subfolder: item.subfolder,
                    title: item.title,
                    href: item.href,
                    indir: item.indir,
                    ek: item.ek
                };

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

                if ((i + 1) % 10 === 0) await sleep(150);
            }
        }
    },


    "tcmb_duyurular": {
        // Distill text -> JSON string: [ { id, title, href }, ... ]
        parseNewData(distillPayload) {
            const { id, name, uri, text, ts, to, dbCollection } = distillPayload;

            const arr = JSON.parse(text);

            const newData = arr
                .map(item => ({
                    duyuru_id: String(item.id || "").trim(),
                    title: String(item.title || "").trim(),
                    // href'i DB'ye yazmayacağız ama mail için payload'ta kalsın
                    href: item.href || null
                }))
                // hem id hem title dolu olmalı
                .filter(x => x.duyuru_id && x.title);

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

        // DB'den eski verileri çek
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
                    offset += limit;
                }
            }

            return allDocs.map(doc => ({
                docId: doc.$id,
                duyuru_id: doc.duyuru_id, // collection'da bu alanı açtın
                title: doc.title
            }));
        },

        // id üzerinden added / removed hesapla
        compare(oldData, newData) {
            const oldIds = new Set(oldData.map(i => i.duyuru_id));
            const newIds = new Set(newData.map(i => i.duyuru_id));

            const added = newData.filter(i => !oldIds.has(i.duyuru_id));
            const removed = oldData.filter(i => !newIds.has(i.duyuru_id));

            // title değişirse eski id için removed + yeni id için added gibi davranıyoruz
            const changed = [];

            return { added, removed, changed };
        },

        // DB senkronu: duyuru_id uniq, title güncellenebilir
        async syncDb(databases, oldData, newData, removed, meta) {
            const oldMap = new Map(oldData.map(i => [i.duyuru_id, i]));

            // removed sil
            for (let i = 0; i < removed.length; i++) {
                const item = removed[i];
                const existing = oldMap.get(item.duyuru_id);

                if (existing?.docId) {
                    await databases.deleteDocument(
                        APPWRITE_DATABASE_ID,
                        meta.dbCollection,
                        existing.docId
                    );
                }
            }

            // newData upsert (duyuru_id aynıysa update, yoksa create)
            for (let i = 0; i < newData.length; i++) {
                const item = newData[i];
                const existing = oldMap.get(item.duyuru_id);

                const payload = {
                    duyuru_id: item.duyuru_id,
                    title: item.title
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
    },


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

    // 4) mode kontrolü (default: direct mail)
    const mode = distillPayload.mode || meta.mode || "direct";

    if (mode === "pool") {
        // ✅ mail atma, pool'a yaz
        await enqueueToPool(databases, meta, {
            meta,
            added,
            removed,
            dbCollectionPool: distillPayload.dbCollectionPool || meta.dbCollectionPool
        });
    } else {
        // ✅ eski davranış aynen devam
        await sendReportMail({ meta, added, removed, changed });
    }

    // 5) sync
    await watcher.syncDb(databases, oldData, newData, removed, meta);

    return { meta, added, removed, changed, mode };
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
