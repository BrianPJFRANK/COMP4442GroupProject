const API_BASE_URL = 'http://comp4442-gp-DrivingBehaviorAPI-env.eba-i62unp3j.us-east-1.elasticbeanstalk.com/api'; // Update to Elastic Beanstalk URL

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
