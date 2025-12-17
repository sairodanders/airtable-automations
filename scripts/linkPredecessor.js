// Airtable Automation script
// Inputs expected via input.config():
// - records: array of record IDs (sorted by previous "Sort list" action) e.g. ["recA","recB", ...]
// - triggerRecordID: the record ID of the record to update (string)
// - triggerPredID: the record ID of the record's predecessor
// - recordsID: list of serial numbers of predecessors, sorted on serial number

const { records = [], triggerRecordID, triggerPredID, recordsID } = input.config();

const TABLE_NAME = 'Elements';
const PREDECESSOR_FIELD = 'predecessor';

if (!Array.isArray(records)) {
    throw new Error('Input "records" must be an array of record IDs.');
}

if (!triggerRecordID) {
    throw new Error('Input "triggerRecordID" is required.');
}

const table = base.getTable(TABLE_NAME);
if (!table) throw new Error(`Table "${TABLE_NAME}" not found.`);

async function main() {
    if (records.length === 0) {
        output.set('status', 'no-records-in-input');
        output.set('topRecordId', null);
        output.set('updated', false);
        return;
    }

    const firstId = records[0];

    if (typeof firstId !== 'string' || !firstId.startsWith('rec')) {
        output.set('status', 'invalid-first-id');
        output.set('firstId', firstId);
        output.set('updated', false);
        return;
    }

    if (recordsID[0] == triggerPredID[0]) {
        output.set('status', 'no-update-predecessor');
        output.set('firstId', firstId);
        output.set('updated', false);
        return;
    }

    // Update the triggering record's predecessor linked-record field.
    // predecessor is a single-link field, so we write an array with one record ID.
    await table.updateRecordAsync(triggerRecordID, {
        [PREDECESSOR_FIELD]: [{ id: firstId }]
    });

    output.set('status', 'success');
    output.set('topRecordId', firstId);
    output.set('updated', true);
}

await main();