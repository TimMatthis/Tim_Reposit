import * as fs from 'fs';
import { SurveyResponse, ConjointData } from './types';

export function parseSurveyFile(filePath: string): SurveyResponse[] {
    const content = fs.readFileSync(filePath, 'utf-8');

    // Split by the start delimiter of each email submission
    // "Submitted through API on page:" seems to be the separator.
    const rawBlocks = content.split(/Submitted through API on page:.*?\n/);

    const results: SurveyResponse[] = [];

    for (const block of rawBlocks) {
        if (!block.trim()) continue;

        const firstNameMatch = block.match(/First Name:\s*\n\n(.*?)\s*\n/);
        const lastNameMatch = block.match(/Last Name:\s*\n\n(.*?)\s*\n/);
        const emailMatch = block.match(/Email:\s*\n\n(.*?)\s*\n/);
        const batteryStatusMatch = block.match(/Battery Status:\s*\n\n(.*?)\s*\n/);
        const batteryBrandMatch = block.match(/Battery Brand:\s*\n\n(.*?)\s*\n/);

        // Extract JSON
        // It starts after "Conjoint Survey Data:" and usually ends before the next field or end of block.
        // We'll look for the first { and the last } in the block segment.
        const jsonStartMarker = "Conjoint Survey Data:";
        const jsonStartIndex = block.indexOf(jsonStartMarker);

        let conjointData: ConjointData | null = null;

        if (jsonStartIndex !== -1) {
            const afterMarker = block.substring(jsonStartIndex + jsonStartMarker.length);
            // Find the first '{'
            const firstBrace = afterMarker.indexOf('{');
            if (firstBrace !== -1) {
                // To find the end, we can try to parse the JSON or look for the matching closing brace.
                // Assuming the JSON is on one line or a contiguous block
                // Let's try to grab everything until the next double newline sequence which usually separates fields
                // OR just grab the line.
                // In the sample, it looks like it's on a single line or wrapped. 
                // Let's find the closing brace that makes valid JSON.

                // Heuristic: The JSON is likely the longest substring starting at `firstBrace` that is valid JSON.
                // Or simply: in the sample it ends before "Battery Status:"

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
                    // console.error("Candidate was:", jsonCandidate);
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
