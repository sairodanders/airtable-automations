/**
 * Inputs from automation:
 * - triggerRecordID: the record to update
 * - triggerPred_date: the predecessor date (Date or ISO string)
 * - triggerDelay: number of working days to add if no manual delay
 * - triggerManualDelay: optional number of working days to override triggerDelay
 */

const table = base.getTable('Elements');

const {
    triggerRecordID,
    triggerPred_date = [],
    triggerDelay,
    triggerManualDelay,
    color
} = input.config();

// Validate inputs
if (!triggerRecordID) throw new Error('triggerRecordID is required');
if (!triggerPred_date) throw new Error('triggerPred_date is required');
if (typeof triggerDelay !== 'number') throw new Error('triggerDelay must be a number');

const baseDate = new Date(triggerPred_date[0]);
if (isNaN(baseDate.getTime())) throw new Error('triggerPred_date is not a valid date');

// Determine which delay to use
const workingDaysToAdd = (typeof triggerManualDelay === 'number' && !isNaN(triggerManualDelay))
    ? triggerManualDelay
    : triggerDelay;

// Function to add working days
function addWorkingDays(startDate, days, colorString) {
    const result = new Date(startDate);
    let added = 0;
    const skipFriday = typeof colorString === 'string' && colorString.slice(-1).toUpperCase() === 'B';

    while (added < days) {
        result.setDate(result.getDate() + 1);
        const day = result.getDay();
        const isWeekend = (day === 0 || day === 6); // Sunday=0, Saturday=6
        const isFriday = day === 5;

        if (!isWeekend && !(skipFriday && isFriday)) {
            added++;
        }
    }
    return result;
}

// Calculate production date
const newProdDate = addWorkingDays(baseDate, workingDaysToAdd, color);

// Update the record
await table.updateRecordAsync(triggerRecordID, {
    'production_date': newProdDate
});

output.set('production_date', newProdDate.toISOString());