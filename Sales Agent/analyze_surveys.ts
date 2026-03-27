import * as fs from 'fs';
import { parseSurveyFile } from './src/analysis/parser';
import { analyzeUtilities } from './src/analysis/analyzer';
import { printReport } from './src/analysis/reporter';

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
