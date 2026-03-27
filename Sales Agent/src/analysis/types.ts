export interface SurveyResponse {
    firstName: string;
    lastName: string;
    email: string;
    batteryStatus: string;
    batteryBrand: string;
    conjointData: ConjointData | null;
}

export interface ConjointData {
    userProfile: {
        batteryStatus: string;
        batteryBrand: string;
    };
    responses: ConjointTaskResponse[];
}

export interface ConjointTaskResponse {
    taskId: number;
    option1: PackageOption;
    option2: PackageOption;
    option3: PackageOption;
    selectedOption: number;
}

export interface PackageOption {
    [key: string]: string | number;
}

export interface AttributeStats {
    shown: number;
    selected: number;
}

// Map: Attribute Name -> Attribute Value (as string) -> Stats
export type UtilityMap = Map<string, Map<string, AttributeStats>>;
