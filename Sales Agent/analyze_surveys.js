
const fs = require('fs');
const path = require('path');

// --- Main Parser Logic ---

function parseSurveyFile(filePath) {
    const content = fs.readFileSync(filePath, 'utf-8');

    // Split by the start delimiter of each email submission
    // "Submitted through API on page:" seems to be the separator.
    const rawBlocks = content.split(/Submitted through API on page:.*?\n/);

    const results = [];

    for (const block of rawBlocks) {
        if (!block.trim()) continue;

        const firstNameMatch = block.match(/First Name:\s*\n\n(.*?)\s*\n/);
        const lastNameMatch = block.match(/Last Name:\s*\n\n(.*?)\s*\n/);
        const emailMatch = block.match(/Email:\s*\n\n(.*?)\s*\n/);
        const batteryStatusMatch = block.match(/Battery Status:\s*\n\n(.*?)\s*\n/);
        const batteryBrandMatch = block.match(/Battery Brand:\s*\n\n(.*?)\s*\n/);

        // Extract JSON
        const jsonStartMarker = "Conjoint Survey Data:";
        const jsonStartIndex = block.indexOf(jsonStartMarker);

        let conjointData = null;

        if (jsonStartIndex !== -1) {
            const afterMarker = block.substring(jsonStartIndex + jsonStartMarker.length);
            const firstBrace = afterMarker.indexOf('{');
            if (firstBrace !== -1) {
                const nextField = afterMarker.indexOf("\n \nBattery Status:");
                let jsonCandidate = "";
                if (nextField !== -1) {
                    jsonCandidate = afterMarker.substring(firstBrace, nextField).trim();
                } else {
                    jsonCandidate = afterMarker.substring(firstBrace).trim();
                }

                try {
                    conjointData = JSON.parse(jsonCandidate);
                } catch (e) {
                    console.error("Failed to parse JSON for user:", emailMatch ? emailMatch[1] : "unknown");
                }
            }
        }

        if (emailMatch) {
            results.push({
                firstName: firstNameMatch ? firstNameMatch[1].trim() : "",
                lastName: lastNameMatch ? lastNameMatch[1].trim() : "",
                email: emailMatch ? emailMatch[1].trim() : "",
                batteryStatus: batteryStatusMatch ? batteryStatusMatch[1].trim() : "",
                batteryBrand: batteryBrandMatch ? batteryBrandMatch[1].trim() : "",
                conjointData: conjointData
            });
        }
    }

    return results;
}

// --- Analysis Logic ---

function analyzeUtilities(surveys) {
    const stats = new Map();

    const increment = (attr, value, type) => {
        const valStr = String(value);
        if (!stats.has(attr)) {
            stats.set(attr, new Map());
        }
        const attrMap = stats.get(attr);
        if (!attrMap.has(valStr)) {
            attrMap.set(valStr, { shown: 0, selected: 0 });
        }
        const s = attrMap.get(valStr);
        if (type === 'shown') s.shown++;
        if (type === 'selected') s.selected++;
    };

    for (const survey of surveys) {
        if (!survey.conjointData || !survey.conjointData.responses) continue;

        for (const task of survey.conjointData.responses) {
            const opts = [task.option1, task.option2, task.option3];

            // Track what was shown
            opts.forEach(opt => {
                for (const [key, val] of Object.entries(opt)) {
                    increment(key, val, 'shown');
                }
            });

            // Track what was selected
            // selectedOption is 1-based index
            const selectedIdx = task.selectedOption - 1;
            if (selectedIdx >= 0 && selectedIdx < 3) {
                const selectedOpt = opts[selectedIdx];
                for (const [key, val] of Object.entries(selectedOpt)) {
                    increment(key, val, 'selected');
                }
            }
        }
    }

    return stats;
}

// --- Reporting ---

function printReport(stats, surveys) {
    console.log(`\n=== SURVEY ANALYSIS REPORT ===`);
    console.log(`Total Responses Processed: ${surveys.length}`);
    console.log(`Valid Conjoint Data Found: ${surveys.filter(s => s.conjointData).length}`);
    console.log(`==============================\n`);

    stats.forEach((valuesMap, attributeName) => {
        console.log(`Attribute: ${attributeName}`);
        console.log(`---------------------------------------------------------------`);
        console.log(`| ${"Value".padEnd(20)} | ${"Win Rate".padEnd(10)} | ${"Selected".padEnd(8)} | ${"Shown".padEnd(8)} |`);
        console.log(`---------------------------------------------------------------`);

        // Sort by win rate descending
        const sortedEntries = Array.from(valuesMap.entries()).sort((a, b) => {
            const rateA = a[1].selected / a[1].shown;
            const rateB = b[1].selected / b[1].shown;
            return rateB - rateA;
        });

        for (const [val, stat] of sortedEntries) {
            const winRate = ((stat.selected / stat.shown) * 100).toFixed(1) + "%";
            console.log(`| ${val.padEnd(20)} | ${winRate.padEnd(10)} | ${String(stat.selected).padEnd(8)} | ${String(stat.shown).padEnd(8)} |`);
        }
        console.log(`\n`);
    });
}

// --- Main Execution ---

const inputFile = process.argv[2] || 'sample_surveys.txt';

if (!fs.existsSync(inputFile)) {
    console.error(`Error: File '${inputFile}' not found.`);
    process.exit(1);
}

try {
    console.log(`Reading from ${inputFile}...`);
    const data = parseSurveyFile(inputFile);

    // Dump Parsed Users for verification
    console.log("Parsed Users:");
    data.forEach(d => console.log(`- ${d.firstName} ${d.lastName} (${d.email}) [Conjoint Data: ${d.conjointData ? 'Yes' : 'No'}]`));

    const utilStats = analyzeUtilities(data);
    printReport(utilStats, data);

} catch (err) {
    console.error("An error occurred:", err);
}
