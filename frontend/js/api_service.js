const API_BASE_URL = '/api';

// Mock Data for development
const MOCK_SUMMARY = {
    "status": "success",
    "data": [
        {
            "driverID": "D01",
            "carPlateNumber": "HK1234",
            "totalOverspeedCount": 12,
            "totalFatigueCount": 1,
            "totalOverspeedTimeSeconds": 340,
            "totalNeutralSlideTimeSeconds": 120
        },
        {
            "driverID": "D02",
            "carPlateNumber": "HK5678",
            "totalOverspeedCount": 5,
            "totalFatigueCount": 0,
            "totalOverspeedTimeSeconds": 100,
            "totalNeutralSlideTimeSeconds": 50
        }
    ]
};

const MOCK_SPEED_DATA = {
    "D01": {
        "status": "success",
        "driverID": "D01",
        "speedData": [
            {"time": "08:00:00", "speed": 85, "isOverspeed": 0},
            {"time": "08:00:30", "speed": 115, "isOverspeed": 1},
            {"time": "08:01:00", "speed": 90, "isOverspeed": 0},
            {"time": "08:01:30", "speed": 105, "isOverspeed": 1},
            {"time": "08:02:00", "speed": 80, "isOverspeed": 0}
        ]
    },
    "D02": {
        "status": "success",
        "driverID": "D02",
        "speedData": [
            {"time": "09:00:00", "speed": 60, "isOverspeed": 0},
            {"time": "09:00:30", "speed": 65, "isOverspeed": 0},
            {"time": "09:01:00", "speed": 70, "isOverspeed": 0}
        ]
    }
};

const ApiService = {
    async getSummary() {
        // Uncomment when backend is ready
        /*
        try {
            const response = await fetch(`${API_BASE_URL}/summary`);
            return await response.json();
        } catch (error) {
            console.error("API Error:", error);
            return MOCK_SUMMARY;
        }
        */
        return MOCK_SUMMARY;
    },

    async getSpeedData(driverID) {
        // Uncomment when backend is ready
        /*
        try {
            const response = await fetch(`${API_BASE_URL}/speed/${driverID}`);
            return await response.json();
        } catch (error) {
            console.error("API Error:", error);
            return MOCK_SPEED_DATA[driverID] || { status: "error", speedData: [] };
        }
        */
        return MOCK_SPEED_DATA[driverID] || { status: "error", speedData: [] };
    }
};
