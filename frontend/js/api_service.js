const API_BASE_URL = 'http://127.0.0.1:8000/api';

const ApiService = {
    async getSummary() {
        try {
            const response = await fetch(`${API_BASE_URL}/summary`);
            return await response.json();
        } catch (error) {
            console.error("API Error (getSummary):", error);
            return { status: "error", data: [] };
        }
    },

    async getSpeedData(driverID) {
        try {
            const response = await fetch(`${API_BASE_URL}/speed/${driverID}`);
            return await response.json();
        } catch (error) {
            console.error(`API Error (getSpeedData for ${driverID}):`, error);
            return { status: "error", speedData: [], warning: false };
        }
    }
};
