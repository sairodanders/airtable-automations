// Airtable Automation script — Upsert + Soft-delete + GenerationID + Audit
// Assumes your base has tables: Bekistingen, Workload Allocation, Afdelingen, Allocation Audit, Errors
// Make sure to create/rename fields as described above before running.

const config = input.config();
const TRIGGER_RECORD_ID = config.recordId;
if (!TRIGGER_RECORD_ID) {
    throw new Error("Missing input.config().recordId. In the Automation action set recordId to the trigger record's ID.");
}

const MAIN_TABLE_NAME = config.mainTableName || "Bekistingen";
const ALLOC_TABLE_NAME = config.allocationsTableName || "Workload Allocation";
const DEPARTMENTS_TABLE_NAME = config.departmentsTableName || "Afdelingen";
const ERRORS_TABLE_NAME = config.errorsTableName || "Errors";
const AUDIT_TABLE_NAME = config.auditTableName || "Allocation Audit";

// Bekistingen table fields
const F_AANTAL_TAFELS = "Aantal Tafels";
const F_LEVERDATUM = "Leverdatum";
const F_AANTAL_GIET = "aantal_giet";
const F_BEK_UREN_CREATE = "BEK_uren_create";
const F_BEK_GEM_U = "BEK_gem_u";
const F_LAS_GEM_U = "LAS_gem_u";
const F_GIET_GEM_U = "GIET_gem_u";
const F_HOF_GEM_REST_UREN = "HOF_gem_rest_per_giet";
const F_TEKEN_F1_DAGEN = "TEK_fase_1_uren";
const F_TEKEN_F2_DAGEN = "TEK_fase_2_uren";
const F_BEKISTINGSGROEP_ID = "Naam";
const F_ALLOC_GENERATED = "Allocations Generated";
const F_LAST_ALLOC_TS = "LastAllocationsGeneratedAt"; // keep this as timestamp

// Allocation table fields
const F_ALLOC_KEY = "Allocation Key"; // must be a writable single-line text field
const F_ALLOC_GENID = "GenerationID";
const F_ALLOC_BEK_LINK = "Bekistingsgroep ID"; // link to main record
const F_ALLOC_BEK_TABLE = "Tafel Index";
const F_ALLOC_AFD = "Afdeling";
const F_ALLOC_ACT = "Activiteit";
const F_ALLOC_HOURS = "Werkuren (totaal)";
const F_ALLOC_START = "Start Datum";
const F_ALLOC_END = "Eind Datum";
const F_ALLOC_DELETED = "Deleted"; // checkbox for soft-delete

const DEPT_BEK = "Bekisting";
const DEPT_LAS = "Lashoek";
const DEPT_BETON = "Beton";
const DEPT_REST = "Rest";
const DEPT_TEK = "Ontwerp";

const ACT_REUSE = "Reuse";
const ACT_CREATE = "Create";
const ACT_LAS = "Las";
const ACT_BETON = "Beton";
const ACT_REST = "Rest";
const ACT_DESIGN_1 = "Ontwerp 1";
const ACT_DESIGN_2 = "Ontwerp 2";

const BATCH_SIZE = 50;

/*
* Controllable parameters for better planning
*
*/
const PRODUCTION_BUFFER_DAYS = 8; // working days before delivery date production should be finished
const BEK_EFF_FACTOR = 0.65;
const TEK_GVP_TO_PRODUCTION_BUFFER = 10; // in working days
const TEK_F1_TO_F2_BUFFER = 10; // in working days
const TEK_CUSTOMER_RESPONSE_BUFFER = 10; // in working days


/* Helper functions */
/**
* @param {string} name
*/
function getTableByNameOrThrow(name) {
    try {
        return base.getTable(name);
    } catch (e) {
        throw new Error(`Table "${name}" not found in this base. Please check the table name in script config.`);
    }
}
/**
* @param {string | number | Date} d
*/
function isWeekend(d) { const day = new Date(d).getDay(); return day === 0 || day === 6; }
/**
* @param {string | number | Date} date
* @param {number} days
*/
function addWorkingDays(date, days) {
    const sign = days >= 0 ? 1 : -1;
    let remaining = Math.abs(days);
    let cur = new Date(date);
    while (remaining > 0) {
        cur.setDate(cur.getDate() + sign);
        if (!isWeekend(cur)) remaining -= 1;
    }
    return cur;
}
/**
* @param {Date} date
* @param {number} days
*/
function subWorkingDays(date, days) { return addWorkingDays(date, -days); }
/**
* @param {string | number | Date} d
*/
function isoDate(d) { return d ? new Date(d).toISOString().split("T")[0] : null; }
/**
* @param {number} v
*/
function ceil(v) { return Math.ceil(v); }

function makeGenerationId() {
    return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
* @param {Table} auditTable
* @param {string} mainRecordId
* @param {string} generationId
* @param {any[]} createdIds
* @param {any[]} updatedIds
* @param {any[]} deletedIds
* @param {{ planned: number; created: number; updated: number; deleted: number; }} details
*/
async function createAuditEntry(auditTable, mainRecordId, generationId, createdIds, updatedIds, deletedIds, details) {
    try {
        const payload = {
            "Timestamp": new Date().toISOString(),
            "Main Record": mainRecordId ? [{ id: mainRecordId }] : undefined,
            "Action": "Upsert",
            "GenerationID": generationId,
            "Details": JSON.stringify(details || {}),
            "Created IDs": createdIds && createdIds.length ? createdIds.join(", ") : "",
            "Updated IDs": updatedIds && updatedIds.length ? updatedIds.join(", ") : "",
            "Deleted IDs": deletedIds && deletedIds.length ? deletedIds.join(", ") : ""
        };
        // remove undefined fields for createRecordAsync
        Object.keys(payload).forEach(k => payload[k] === undefined && delete payload[k]);
        //console.debug(JSON.stringify(payload));
        await auditTable.createRecordAsync(payload);
    } catch (e) {
        console.warn("Failed to write audit entry:", e);
    }
}

(async () => {
    // small helper for error logging to Errors table (tries link first)
    /**
  * @param {Table} errorsTableObj
  * @param {string} sourceRecordId
  * @param {unknown} message
  * @param {unknown} details
  */
    async function logErrorToErrorsTable(errorsTableObj, sourceRecordId, message, details) {
        try {
            const payload = {
                "Timestamp": new Date().toISOString(),
                "Error Message": message,
                "Details": details || ""
            };
            try {
                payload["Source Record"] = [{ id: sourceRecordId }];
                await errorsTableObj.createRecordAsync(payload);
                return;
            } catch (e) {
                delete payload["Source Record"];
                payload["Source Record (text)"] = sourceRecordId;
                await errorsTableObj.createRecordAsync(payload);
                return;
            }
        } catch (finalErr) {
            console.error("Failed to write to Errors table:", finalErr, "Original:", message, details);
        }
    }

    try {
        const mainTable = getTableByNameOrThrow(MAIN_TABLE_NAME);
        const allocTable = getTableByNameOrThrow(ALLOC_TABLE_NAME);
        const departmentsTable = getTableByNameOrThrow(DEPARTMENTS_TABLE_NAME);
        const errorsTable = getTableByNameOrThrow(ERRORS_TABLE_NAME);
        const auditTable = getTableByNameOrThrow(AUDIT_TABLE_NAME);

        const mainRecord = await mainTable.selectRecordAsync(TRIGGER_RECORD_ID);
        if (!mainRecord) throw new Error(`Trigger record with ID ${TRIGGER_RECORD_ID} not found.`);

        // Early exit if already generated and you choose to skip (optional)
        const alreadyGenerated = !!mainRecord.getCellValue(F_ALLOC_GENERATED);
        // NOTE: we still proceed to upsert even if this flag is true; comment out next line if you want to always do upsert on re-run
        // if (alreadyGenerated) { console.log("Allocations already generated. Exiting."); return; }

        // Read main record fields
        const aantalTafels = mainRecord.getCellValue(F_AANTAL_TAFELS);
        const leverDatum = mainRecord.getCellValue(F_LEVERDATUM);
        const aantalGiet = mainRecord.getCellValue(F_AANTAL_GIET);
        const bekUrenCreate = mainRecord.getCellValue(F_BEK_UREN_CREATE);
        const bekGemU = mainRecord.getCellValue(F_BEK_GEM_U);
        const lasGemU = mainRecord.getCellValue(F_LAS_GEM_U);
        const gietGemU = mainRecord.getCellValue(F_GIET_GEM_U);
        const hofGemRestU = mainRecord.getCellValue(F_HOF_GEM_REST_UREN);
        const bekistingsgroepId = mainRecord.getCellValue(F_BEKISTINGSGROEP_ID);
        const tekenenFase1Dagen = mainRecord.getCellValue(F_TEKEN_F1_DAGEN);
        const tekenenFase2Dagen = mainRecord.getCellValue(F_TEKEN_F2_DAGEN);

        const missing = [];
        if (!aantalTafels || typeof aantalTafels !== "number" || aantalTafels <= 0) missing.push(F_AANTAL_TAFELS);
        if (!leverDatum) missing.push(F_LEVERDATUM);
        if (aantalGiet === null || aantalGiet === undefined) missing.push(F_AANTAL_GIET);
        if (bekUrenCreate === null || bekUrenCreate === undefined) missing.push(F_BEK_UREN_CREATE);
        if (bekGemU === null || bekGemU === undefined) missing.push(F_BEK_GEM_U);
        if (lasGemU === null || lasGemU === undefined) missing.push(F_LAS_GEM_U);
        if (gietGemU === null || gietGemU === undefined) missing.push(F_GIET_GEM_U);
        if (hofGemRestU === null || hofGemRestU === undefined) missing.push(F_HOF_GEM_REST_UREN);
        if (bekistingsgroepId === null || bekistingsgroepId === undefined) missing.push(F_BEKISTINGSGROEP_ID);
        if (tekenenFase1Dagen === null || tekenenFase1Dagen === undefined) missing.push(F_TEKEN_F1_DAGEN);
        if (tekenenFase2Dagen === null || tekenenFase2Dagen === undefined) missing.push(F_TEKEN_F2_DAGEN);

        if (missing.length > 0) {
            const msg = `Missing/invalid on main record: ${missing.join(", ")}`;
            await logErrorToErrorsTable(errorsTable, mainRecord.id, msg, JSON.stringify({ aantalTafels, leverDatum }));
            throw new Error(msg);
        }

        const leverDateObj = new Date(leverDatum);

        // compute dates
        const eindRest = subWorkingDays(leverDateObj, PRODUCTION_BUFFER_DAYS);
        // Keep production every other day
        const startRest = subWorkingDays(eindRest, (aantalGiet * 2) - 1);
        const eindBeton = subWorkingDays(eindRest, 1);
        const startBeton = subWorkingDays(eindBeton, (aantalGiet * 2) - 1);
        const eindLas = subWorkingDays(eindBeton, 1);
        const startLas = subWorkingDays(eindLas, (aantalGiet * 2) - 1);
        // These dates will be calculated. If no more than 1 production day, these rows should not be created
        const eindReuse = subWorkingDays(eindBeton, 1);
        const startReuse = addWorkingDays(startBeton, 1);

        const eindCreate = subWorkingDays(startBeton, 1);
        // productivity factor in percentage of the day that can be used to generate new molds. Currently = 0.65
        const daysNeededCreate = ceil((bekUrenCreate / BEK_EFF_FACTOR) / 8);
        const startCreate = subWorkingDays(eindCreate, daysNeededCreate);

        const eindDesign2 = subWorkingDays(startCreate, TEK_GVP_TO_PRODUCTION_BUFFER);
        const startDesign2 = subWorkingDays(eindDesign2, Math.ceil(tekenenFase2Dagen / 8) + TEK_CUSTOMER_RESPONSE_BUFFER);
        const eindDesign1 = subWorkingDays(startDesign2, 1);
        const startDesign1 = subWorkingDays(eindDesign1, Math.ceil(tekenenFase1Dagen / 8) + TEK_CUSTOMER_RESPONSE_BUFFER);

        const hoursRest = (aantalGiet * hofGemRestU);
        const hoursBeton = (aantalGiet * gietGemU);
        const hoursLas = (aantalGiet * lasGemU);
        const hoursReuse = ((aantalGiet - 1) * bekGemU);
        const hoursCreate = bekUrenCreate;
        const daysDesign1 = tekenenFase1Dagen;
        const daysDesign2 = tekenenFase2Dagen;

        console.debug(`Leverdatum: ${leverDateObj}`);
        console.debug(`Eind rest: ${eindRest}`);
        console.debug(`Start rest: ${startRest}`);
        console.debug(`Eind beton: ${eindBeton}`);
        console.debug(`Start beton: ${startBeton}`);
        console.debug(`Eind las: ${eindLas}`);
        console.debug(`Start las: ${startLas}`);
        console.debug(`Eind create: ${eindCreate}`);
        console.debug(`Start create: ${startCreate}`);

        // Load departments and map names to IDs
        const deptRecords = await departmentsTable.selectRecordsAsync({ fields: ["Name"] });
        const deptNameToId = {};
        for (const r of deptRecords.records) {
            deptNameToId[r.name] = r.id;
        }

        const requiredDepts = [DEPT_BEK, DEPT_LAS, DEPT_BETON, DEPT_REST, DEPT_TEK];
        const missingDepts = requiredDepts.filter(d => !deptNameToId[d]);
        if (missingDepts.length > 0) {
            const msg = `Missing departments: ${missingDepts.join(", ")}`;
            await logErrorToErrorsTable(errorsTable, mainRecord.id, msg, "Ensure Departments table contains these primary names.");
            throw new Error(msg);
        }

        // Validate single-select options for Activiteit (best-effort)
        let allocFieldObj;
        try { allocFieldObj = allocTable.getField(F_ALLOC_ACT); } catch (e) { allocFieldObj = null; }
        const requiredActs = [ACT_REUSE, ACT_CREATE, ACT_LAS, ACT_BETON, ACT_REST, ACT_DESIGN_1, ACT_DESIGN_2];
        let choices = [];
        try {
            const possibleChoices = allocFieldObj?.options?.choices;
            if (Array.isArray(possibleChoices)) choices = possibleChoices.map(c => c?.name).filter(Boolean);
        } catch (e) {
            choices = [];
        }
        if (choices.length > 0) {
            const missingActs = requiredActs.filter(a => !choices.includes(a));
            if (missingActs.length > 0) {
                const msg = `Missing Activiteit options in Workload Allocations single-select: ${missingActs.join(", ")}`;
                await logErrorToErrorsTable(errorsTable, mainRecord.id, msg, "Add these options to the single-select field 'Activiteit'.");
                throw new Error(msg);
            }
        }

        // Build planned rows (same as before, but we will upsert)
        /**
    * @param {any} groupId
    * @param {number} tafelIndex
    * @param {string} deptName
    * @param {string} activityName
    */
        function buildAllocKey(groupId, tafelIndex, deptName, activityName) { return `${groupId || "G"}-T${tafelIndex}-${deptName}-${activityName}`; }

        const groupLinkValue = [{ id: mainRecord.id }];
        const tafelsCount = Math.floor(aantalTafels);
        const plannedRows = [];
        const makeFields = (/** @type {any} */ deptId, /** @type {{ name: string; }} */ activityName, /** @type {number} */ hours, /** @type {string | number | Date} */ startDate, /** @type {string | number | Date} */ endDate, /** @type {number} */ tafelIndex) => {
            const f = {};
            if (groupLinkValue && groupLinkValue.length) f[F_ALLOC_BEK_LINK] = groupLinkValue;
            if (deptId) f[F_ALLOC_AFD] = [{ id: deptId }];
            if (activityName) f[F_ALLOC_ACT] = activityName;
            if (typeof hours === "number") f[F_ALLOC_HOURS] = hours;
            const sIso = isoDate(startDate);
            const eIso = isoDate(endDate);
            if (sIso) f[F_ALLOC_START] = sIso;
            if (eIso) f[F_ALLOC_END] = eIso;
            f[F_ALLOC_BEK_TABLE] = tafelIndex;
            return f;
        };

        for (let t = 1; t <= tafelsCount; t++) {
            plannedRows.push({
                allocKey: buildAllocKey(bekistingsgroepId, t, DEPT_REST, ACT_REST),
                fields: makeFields(deptNameToId[DEPT_REST], { name: ACT_REST }, hoursRest, startRest, eindRest, t)
            });
            plannedRows.push({
                allocKey: buildAllocKey(bekistingsgroepId, t, DEPT_BETON, ACT_BETON),
                fields: makeFields(deptNameToId[DEPT_BETON], { name: ACT_BETON }, hoursBeton, startBeton, eindBeton, t)
            });
            plannedRows.push({
                allocKey: buildAllocKey(bekistingsgroepId, t, DEPT_LAS, ACT_LAS),
                fields: makeFields(deptNameToId[DEPT_LAS], { name: ACT_LAS }, hoursLas, startLas, eindLas, t)
            });
            // Only generate a record when reuses are applicable ('aantal_giet > 1' is the same as 'hoursReuse ==0')
            if (hoursReuse !== 0) {
                plannedRows.push({
                    allocKey: buildAllocKey(bekistingsgroepId, t, DEPT_BEK, ACT_REUSE),
                    fields: makeFields(deptNameToId[DEPT_BEK], { name: ACT_REUSE }, hoursReuse, startReuse, eindReuse, t)
                });
            }
            plannedRows.push({
                allocKey: buildAllocKey(bekistingsgroepId, t, DEPT_BEK, ACT_CREATE),
                fields: makeFields(deptNameToId[DEPT_BEK], { name: ACT_CREATE }, hoursCreate, startCreate, eindCreate, t)
            });
        }



        plannedRows.push({
            allocKey: buildAllocKey(bekistingsgroepId, 1, DEPT_TEK, ACT_DESIGN_1),
            fields: makeFields(deptNameToId[DEPT_TEK], { name: ACT_DESIGN_1 }, daysDesign1, startDesign1, eindDesign1, 1)
        });

        plannedRows.push({
            allocKey: buildAllocKey(bekistingsgroepId, 1, DEPT_TEK, ACT_DESIGN_2),
            fields: makeFields(deptNameToId[DEPT_TEK], { name: ACT_DESIGN_2 }, daysDesign2, startDesign2, eindDesign2, 1)
        });
        // console.debug(JSON.stringify(plannedRows));
        //console.debug(`bekistingsgroepId: ${bekistingsgroepId}`);

        // Generate Generation ID and write to main record (so audit and allocations can reference it)
        const generationId = makeGenerationId();
        await mainTable.updateRecordAsync(mainRecord.id, { [F_ALLOC_GENERATED]: true, [F_LAST_ALLOC_TS]: new Date().toISOString(), [F_ALLOC_GENID]: generationId }).catch(e => {
            // If main table doesn't have Generation ID field, we still continue; log warning
            console.warn("Could not write Generation ID to main record (field may be missing):", e);
        });

        // Helper: fetch existing allocations linked to this main record; returns map allocKey -> record object
        /**
    * @param {string} mainRecId
    */
        async function fetchExistingAllocationsLinkedToMain(mainRecId) {
            const map = new Map();
            const fieldsToFetch = [
                F_ALLOC_KEY, F_ALLOC_GENID, F_ALLOC_BEK_LINK, F_ALLOC_BEK_TABLE,
                F_ALLOC_AFD, F_ALLOC_ACT, F_ALLOC_HOURS, F_ALLOC_START, F_ALLOC_END, F_ALLOC_DELETED
            ];
            const records = await allocTable.selectRecordsAsync({ fields: fieldsToFetch });
            for (const r of records.records) {
                const link = r.getCellValue(F_ALLOC_BEK_LINK);
                if (link && Array.isArray(link) && link.some(l => l.id === mainRecId)) {
                    const key = r.getCellValue(F_ALLOC_KEY);
                    if (typeof key === "string" && key.length > 0) {
                        map.set(key, r);
                    }
                }
            }
            return map;
        }

        // Actually fetch them
        const existingMap = await fetchExistingAllocationsLinkedToMain(mainRecord.id);

        // Build operations: creates, updates, and mark existing as matched or not
        const toCreate = [];
        const toUpdate = [];
        const matchedExistingIds = new Set();

        // compare function to detect differences
        /**
    * @param {any} existingRecord
    * @param {{ [x: string]: any; }} plannedFields
    */
        function needsUpdate(existingRecord, plannedFields) {
            const ef = existingRecord;
            // compare relevant fields (afdeling id, activiteit name, hours, start, end, tafel index)
            /*const getExistingVal = (name) => {
              const val = ef.getCellValue(name);
              return val;
            };*/
            // Afdeling is linked record -> compare first id
            /*const existingAfdeling = (() => {
              const v = ef.getCellValue(F_ALLOC_AFD);
              return Array.isArray(v) && v[0] ? v[0].id : null;
            })();
            const plannedAfdeling = plannedFields[F_ALLOC_AFD] ? plannedFields[F_ALLOC_AFD][0].id : null;
            if ((existingAfdeling || null) !== (plannedAfdeling || null)) return true;
            
      
            // Activiteit (single-select text) compare
            const existingAct = ef.getCellValue(F_ALLOC_ACT);
            const plannedAct = plannedFields[F_ALLOC_ACT];
            if ((existingAct || null) !== (plannedAct || null)) return true;
            */
            // Hours
            const existingHours = ef.getCellValue(F_ALLOC_HOURS);
            const plannedHours = plannedFields[F_ALLOC_HOURS];
            if ((existingHours || null) !== (plannedHours || null)) return true;

            // Start and End dates: Airtable returns ISO strings for date-only fields
            const existingStart = ef.getCellValue(F_ALLOC_START) ? new Date(ef.getCellValue(F_ALLOC_START)).toISOString().split("T")[0] : null;
            const plannedStart = plannedFields[F_ALLOC_START] ? plannedFields[F_ALLOC_START] : null;
            if ((existingStart || null) !== (plannedStart || null)) return true;

            const existingEnd = ef.getCellValue(F_ALLOC_END) ? new Date(ef.getCellValue(F_ALLOC_END)).toISOString().split("T")[0] : null;
            const plannedEnd = plannedFields[F_ALLOC_END] ? plannedFields[F_ALLOC_END] : null;
            if ((existingEnd || null) !== (plannedEnd || null)) return true;

            // Tafel Index
            const existingTafel = ef.getCellValue(F_ALLOC_BEK_TABLE);
            const plannedTafel = plannedFields[F_ALLOC_BEK_TABLE];
            if ((existingTafel || null) !== (plannedTafel || null)) return true;

            // If none differ
            return false;
        }

        // Prepare planned rows for upsert
        for (const pr of plannedRows) {
            const key = pr.allocKey;
            const plannedFields = Object.assign({}, pr.fields);
            // Always include the Generation ID for created/updated rows so we can prune stale ones later
            plannedFields[F_ALLOC_GENID] = generationId;
            // Also write Allocation Key into record on create/update
            plannedFields[F_ALLOC_KEY] = key;

            const existing = existingMap.get(key);
            if (existing) {
                // Exists: maybe update
                if (needsUpdate(existing, plannedFields) || (existing.getCellValue(F_ALLOC_GENID) !== generationId) || existing.getCellValue(F_ALLOC_DELETED)) {
                    // update: provide record id and fields
                    toUpdate.push({ id: existing.id, fields: plannedFields });
                } else {
                    // still ensure generation id is set if missing
                    if (!existing.getCellValue(F_ALLOC_GENID)) {
                        toUpdate.push({ id: existing.id, fields: { [F_ALLOC_GENID]: generationId } });
                    }
                }
                matchedExistingIds.add(existing.id);
            } else {
                // Not exists: create
                toCreate.push({ fields: plannedFields });
            }
        }

        // Any existing allocations linked to this main record that were NOT matched should be considered stale
        const staleExisting = [];
        for (const [key, rec] of existingMap.entries()) {
            if (!matchedExistingIds.has(rec.id)) staleExisting.push(rec);
        }

        // Now perform the actual DB operations in safe batches (create, update, then soft-delete)
        const createdIds = [];
        const updatedIds = [];
        const deletedIds = [];

        // Helper to process create batches
        for (let i = 0; i < toCreate.length; i += BATCH_SIZE) {
            const batch = toCreate.slice(i, i + BATCH_SIZE);
            // Before creating, re-check if any of these keys were added by a concurrent process
            const currentMap = await fetchExistingAllocationsLinkedToMain(mainRecord.id);
            const batchFiltered = [];
            for (const item of batch) {
                const key = item.fields[F_ALLOC_KEY];
                if (!currentMap.has(key)) batchFiltered.push(item);
                else {
                    // If it exists now, consider it for update instead of create
                    const existing = currentMap.get(key);
                    if (needsUpdate(existing, item.fields)) {
                        toUpdate.push({ id: existing.id, fields: item.fields });
                    }
                }
            }
            if (batchFiltered.length === 0) continue;
            //console.debug(`This is the content: ${JSON.stringify(batchFiltered)}`);
            const created = await allocTable.createRecordsAsync(batchFiltered);
            // createRecordsAsync returns an array of created record objects; extract ids
            /* for (const r of created) {
               createdIds.push(r.id);
             }*/
            createdIds.push(created);
        }

        // Helper to process update batches
        for (let i = 0; i < toUpdate.length; i += BATCH_SIZE) {
            const batch = toUpdate.slice(i, i + BATCH_SIZE);
            //console.debug(JSON.stringify(batch));
            try {
                await allocTable.updateRecordsAsync(batch);
                for (const b of batch) updatedIds.push(b.id);
            } catch (e) {
                // retry individually (best-effort)
                console.warn("Batch update failed; retrying individually:", e);
                for (const b of batch) {
                    try {
                        await allocTable.updateRecordAsync(b.id, b.fields);
                        updatedIds.push(b.id);
                    } catch (errSingle) {
                        console.error("Failed to update record", b.id, errSingle);
                        await logErrorToErrorsTable(errorsTable, mainRecord.id, `Failed to update allocation ${b.id}`, String(errSingle));
                    }
                }
            }
        }

        // Soft-delete stale allocations: set Deleted checkbox to true and set Generation ID (so we know when it was pruned)
        // You might choose to hard delete instead — be careful.
        const softDeleteOps = [];
        for (const rec of staleExisting) {
            // only soft-delete if not already marked deleted
            if (!rec.getCellValue(F_ALLOC_DELETED)) {
                softDeleteOps.push({ id: rec.id, fields: { [F_ALLOC_DELETED]: true, [F_ALLOC_GENID]: generationId } });
            } else {
                // still update generation id if needed
                if (rec.getCellValue(F_ALLOC_GENID) !== generationId) {
                    softDeleteOps.push({ id: rec.id, fields: { [F_ALLOC_GENID]: generationId } });
                }
            }
        }
        for (let i = 0; i < softDeleteOps.length; i += BATCH_SIZE) {
            const batch = softDeleteOps.slice(i, i + BATCH_SIZE);
            try {
                await allocTable.updateRecordsAsync(batch);
                for (const u of batch) deletedIds.push(u.id);
            } catch (e) {
                console.warn("Batch soft-delete failed; retrying individually:", e);
                for (const u of batch) {
                    try {
                        await allocTable.updateRecordAsync(u.id, u.fields);
                        deletedIds.push(u.id);
                    } catch (errSingle) {
                        console.error("Failed to soft-delete record", u.id, errSingle);
                        await logErrorToErrorsTable(errorsTable, mainRecord.id, `Failed to soft-delete allocation ${u.id}`, String(errSingle));
                    }
                }
            }
        }

        // Write an audit entry summarizing this run
        try {
            await createAuditEntry(auditTable, mainRecord.id, generationId, createdIds, updatedIds, deletedIds, {
                planned: plannedRows.length,
                created: createdIds.length,
                updated: updatedIds.length,
                deleted: deletedIds.length
            });
        } catch (e) {
            console.warn("Failed to create audit entry:", e);
        }

        // Final update on main record: flag and timestamp + generation id already written earlier (best-effort)
        await mainTable.updateRecordAsync(mainRecord.id, { [F_ALLOC_GENERATED]: true, [F_LAST_ALLOC_TS]: new Date().toISOString(), [F_ALLOC_GENID]: generationId }).catch(e => {
            console.warn("Could not update main record with final timestamp/generation ID", e);
        });

        console.log(`Upsert complete. Created ${createdIds.length}, updated ${updatedIds.length}, soft-deleted ${deletedIds.length}. GenerationID: ${generationId}`);

    } catch (err) {
        try {
            const errorsTable = base.getTable(ERRORS_TABLE_NAME);
            await logErrorToErrorsTable(errorsTable, TRIGGER_RECORD_ID, err.message || String(err), err.stack || "");
        } catch (logErr) {
            console.error("Failed to log in Errors table: ", logErr);
        }
        throw err;
    }
})();

