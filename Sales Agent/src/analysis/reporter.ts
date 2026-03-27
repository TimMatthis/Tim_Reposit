import { UtilityMap, SurveyResponse } from './types';

export function printReport(stats: UtilityMap, surveys: SurveyResponse[]) {
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
