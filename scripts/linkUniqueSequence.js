// Get input config
let inputConfig = input.config();
let elementsTable = base.getTable("Elements");
let sequencesTable = base.getTable("Sequences");
let projectsTable = base.getTable("Projects");

let recordId = inputConfig.recordId;
let sequencePicker = inputConfig.Sequence_number;
let project = inputConfig.project;

await main()

async function main() {
    // If picker is empty, clear linked sequence and exit
    if (!sequencePicker) {
        await elementsTable.updateRecordAsync(recordId, { "Sequences": [] });
        output.set("status", "Cleared sequence link");
        return;
    }

    // Build unique key
    let sequenceNumber = parseInt(sequencePicker);
    let uniqueKey = `${project}-${sequenceNumber}`;
    console.log(uniqueKey);

    // Look for existing sequence
    let query = await sequencesTable.selectRecordsAsync({
        fields: ["Sequence_name"]
    });

    let existing = query.records.find(r => r.getCellValue("Sequence_name") === uniqueKey);
    console.log(existing);

    let sequenceId;

    // If sequence exists, use it
    if (existing) {
        sequenceId = existing.id;
    } else {
        // Otherwise look for the project
        let query_project = await projectsTable.selectRecordsAsync({
            fields: ["delivery_address"]
        });
        let existing_project = query_project.records.find(p => p.getCellValue("delivery_address") === project[0]);

        if (!existing_project) {
            output.set("Status", "First create the project");
            return;
        } else {
            let created = await sequencesTable.createRecordAsync({
                "sequence_number": sequenceNumber,
                "worksite_id": [{ id: existing_project.id }],
            }
            );
            sequenceId = created;
        }
    }

    await elementsTable.updateRecordAsync(recordId, { "Sequences": [{ id: sequenceId }] });

}
