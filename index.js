import { Client, Databases, ID } from "node-appwrite";
import * as cheerio from "cheerio";

// =====================
//  CONFIG (.env'den)
// =====================

const APPWRITE_ENDPOINT = process.env.APPWRITE_ENDPOINT;
const APPWRITE_PROJECT_ID = process.env.APPWRITE_PROJECT_ID;
const APPWRITE_API_KEY = process.env.APPWRITE_API_KEY;
const APPWRITE_DATABASE_ID = process.env.DATABASE_ID;
const APPWRITE_COLLECTION_ID = process.env.COLLECTION_ID;

// Web’den veri çekilecek URL
const WEB_URL = "https://www.tcmb.gov.tr/wps/wcm/connect/tr/tcmb+tr/main+menu/temel+faaliyetler/odeme+hizmetleri/odeme+kuruluslari";

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
        docId: doc.$id,                // 🔴 BURASI YENİ
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
    const harfler = ["a", "b", "c", "ç", "d", "e", "f", "g"];

    $("tbody > tr").slice(2).each((_, tr) => {
        const tds = $(tr).find("td");
        if (tds.length < 3) return;

        const kurulus_kodu = $(tds[1]).text().trim();
        let kurulus_adi = $(tds[2]).text().trim();
        kurulus_adi = kurulus_adi.replace(/\s+/g, " ");

        const yetkiler = [];

        for (let i = 0; i < harfler.length; i++) {
            const cellIndex = 3 + i;
            if (cellIndex >= tds.length) break;

            let cellText = $(tds[cellIndex]).text();
            cellText = cellText.replace(/\u00a0/g, "").trim();

            if (cellText !== "") {
                yetkiler.push(harfler[i]);
            }
        }

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

  // 🔴 ÖNEMLİ: Ortakları newData'dan alıyoruz (yeni snapshot)
  const commonKuruluslar = newData.filter(item => {
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
    degisenler3,
  };
}

async function syncDbWithNewData(databases, dbData, newData, removed) {
    const dbDataByCode = new Map(
        dbData.map(item => [item.kurulus_kodu, item])
    );

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




function escapeHtml(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function renderRows(items) {
  if (!items || items.length === 0) {
    return `
      <tr>
        <td style="padding:8px;border-top:1px solid #d4d4d4;" colspan="3">
          Kayıt bulunamadı.
        </td>
      </tr>
    `;
  }

  return items
    .map(item => {
      const yetkiText =
        item.yetkiler && item.yetkiler.length
          ? item.yetkiler.join(", ")
          : "-";

      return `
        <tr>
          <td style="padding:8px;border-top:1px solid #d4d4d4;">${escapeHtml(
            item.kurulus_kodu
          )}</td>
          <td style="padding:8px;border-top:1px solid #d4d4d4;">${escapeHtml(
            item.kurulus_adi
          )}</td>
          <td style="padding:8px;border-top:1px solid #d4d4d4;">${escapeHtml(
            yetkiText
          )}</td>
        </tr>
      `;
    })
    .join("");
}

function buildEmailHtml(added, removed, changed) {
  const addedRows = renderRows(added);
  const removedRows = renderRows(removed);
  const changedRows = renderRows(changed);

  const today = new Date().toLocaleDateString("tr-TR");

  return `
<!DOCTYPE html>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Yeni Değişiklikler</title>
  </head>
  <body
    style="margin:0;padding:0;background-color:#ffffff;font-family:Arial,Helvetica,sans-serif;"
  >
    <table
      width="100%"
      cellpadding="0"
      cellspacing="0"
      border="0"
      style="background-color:#ffffff;"
    >
      <tr>
        <td align="center">
          <table
            width="600"
            cellpadding="0"
            cellspacing="0"
            border="0"
            style="width:600px;max-width:600px;border:1px solid #d4d4d4;background-color:#ffffff;"
          >
            <!-- Header -->
            <tr>
              <td
                align="center"
                style="background-color:#d4d4d4;padding:16px 0 12px 0;"
              >
                <img
                  src="https://raw.githubusercontent.com/alpbayram/todeb-mail/refs/heads/main/TODEB_Logo.png"
                  alt="TODEB Logo"
                  width="280"
                  height="auto"
                  style="display:block;border:none;outline:none;text-decoration:none;"
                />
              </td>
            </tr>
            <tr>
              <td
                align="center"
                style="background-color:#d4d4d4;padding:8px 24px 12px 24px;"
              >
                <h1
                  style="margin:0;font-size:24px;font-weight:bold;color:#000000;"
                >
                  Yeni Değişiklikler
                </h1>
                <p
                  style="margin:8px 0 0 0;font-size:14px;color:#333333;font-weight:bold;"
                >
                  Son güncellemeler aşağıda listelenmiştir.
                </p>
              </td>
            </tr>

            <!-- Spacer -->
            <tr><td height="24" style="font-size:0;line-height:0;">&nbsp;</td></tr>

            <!-- YENİ EKLENENLER -->
            <tr>
              <td style="padding:0 24px 16px 24px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td
                      style="font-size:18px;font-weight:bold;color:#000000;padding-bottom:8px;"
                    >
                      Yeni Eklenenler
                    </td>
                  </tr>
                  <tr>
                    <td
                      style="border:1px solid #d4d4d4;padding:0;font-size:14px;color:#405464;"
                    >
                      <table
                        width="100%"
                        cellpadding="0"
                        cellspacing="0"
                        border="0"
                        style="border-collapse:collapse;"
                      >
                        <thead>
                          <tr>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Kuruluş Kodu
                            </th>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Kuruluş Adı
                            </th>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Yetkileri
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          ${addedRows}
                        </tbody>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- SİLİNENLER -->
            <tr>
              <td style="padding:0 24px 16px 24px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td
                      style="font-size:18px;font-weight:bold;color:#000000;padding-bottom:8px;"
                    >
                      Silinenler
                    </td>
                  </tr>
                  <tr>
                    <td
                      style="border:1px solid #d4d4d4;padding:0;font-size:14px;color:#405464;"
                    >
                      <table
                        width="100%"
                        cellpadding="0"
                        cellspacing="0"
                        border="0"
                        style="border-collapse:collapse;"
                      >
                        <thead>
                          <tr>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Kuruluş Kodu
                            </th>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Kuruluş Adı
                            </th>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Yetkileri
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          ${removedRows}
                        </tbody>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- DEĞİŞENLER -->
            <tr>
              <td style="padding:0 24px 24px 24px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td
                      style="font-size:18px;font-weight:bold;color:#000000;padding-bottom:8px;"
                    >
                      Değişenler
                    </td>
                  </tr>
                  <tr>
                    <td
                      style="border:1px solid #d4d4d4;padding:0;font-size:14px;color:#405464;"
                    >
                      <table
                        width="100%"
                        cellpadding="0"
                        cellspacing="0"
                        border="0"
                        style="border-collapse:collapse;"
                      >
                        <thead>
                          <tr>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Kuruluş Kodu
                            </th>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Kuruluş Adı
                            </th>
                            <th
                              align="left"
                              style="padding:8px;border-bottom:1px solid #d4d4d4;font-size:13px;font-weight:bold;background-color:#f5f5f5;"
                            >
                              Yetkileri
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          ${changedRows}
                        </tbody>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td
                align="center"
                style="background-color:#f0f0f0;padding:12px;font-size:12px;color:#666666;"
              >
              WebWatcher Otomatik Bildirim • ${today}
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;
}


// =====================
//  MAIL FUNCTION ÇAĞIRMA
// =====================

async function sendReportMail({ added, removed, changed }) {
  const html = buildEmailHtml(added, removed, changed);

  await fetch(MAIL_FUNCTION_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      added,
      removed,
      changed,
      to: "alp.bayram@todeb.org.tr",
      subject: "WebWatcher Güncelleme Raporu",
      html
    })
  });
}


// =====================
//  ANA ÇALIŞTIRMA
// =====================

async function run() {
  const { databases } = createClient();

  const dbData = await getDbData(databases);      // oldData
  const newData = await getNewDataFromWeb();      // newData (web tablosu)

  const { added, removed } = compareKuruluslar(dbData, newData);
  log(removed);
  const commonKuruluslar = getCommonKuruluslar(dbData, newData);
  const { degisenler3 } = kontrolEt(commonKuruluslar, dbData);
await syncDbWithNewData(databases, dbData, newData, removed);
  await sendReportMail({
    added,
    removed,
    changed: degisenler3,
  });
  
  return {
    added,
    removed,
    changed: degisenler3,
  };
}

// =====================
//  APPWRITE FUNCTION HANDLER
// =====================

export default async ({ req, res, log, error }) => {
  try {
    const result = await run();

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











