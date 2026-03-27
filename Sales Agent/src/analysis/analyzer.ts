import { SurveyResponse, UtilityMap } from './types';

export function analyzeUtilities(surveys: SurveyResponse[]): UtilityMap {
    const stats: UtilityMap = new Map();

    const increment = (attr: string, value: string | number, type: 'shown' | 'selected') => {
        const valStr = String(value);
        if (!stats.has(attr)) {
            stats.set(attr, new Map());
        }
        const attrMap = stats.get(attr)!;
        if (!attrMap.has(valStr)) {
            attrMap.set(valStr, { shown: 0, selected: 0 });
        }
        const s = attrMap.get(valStr)!;
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
