let speedChart = null;
let currentDriverID = null;
let refreshInterval = null;

document.addEventListener('DOMContentLoaded', async () => {
    await initDashboard();
});

async function initDashboard() {
    const summary = await ApiService.getSummary();
    renderDriverList(summary.data);
    
    // Select first driver by default if available
    if (summary.data.length > 0) {
        selectDriver(summary.data[0].driverID);
    }

    // Set 30s refresh as per plan
    refreshInterval = setInterval(async () => {
        if (currentDriverID) {
            await updateSpeedData(currentDriverID);
        }
        const updatedSummary = await ApiService.getSummary();
        renderDriverSummaryInfo(updatedSummary.data.find(d => d.driverID === currentDriverID));
    }, 30000);
}

function renderDriverList(drivers) {
    const listElement = document.getElementById('driver-list');
    listElement.innerHTML = '';

    drivers.forEach(driver => {
        const item = document.createElement('a');
        item.className = `list-group-item list-group-item-action ${driver.driverID === currentDriverID ? 'active' : ''}`;
        item.innerHTML = `
            <div class="d-flex w-100 justify-content-between">
                <h6 class="mb-1">${driver.driverID}</h6>
                <small>${driver.carPlateNumber}</small>
            </div>
            <p class="mb-1 text-sm">Overspeed: ${driver.totalOverspeedCount}</p>
        `;
        item.onclick = () => selectDriver(driver.driverID);
        item.setAttribute('data-driver-id', driver.driverID);
        listElement.appendChild(item);
    });
}

async function selectDriver(driverID) {
    currentDriverID = driverID;
    document.getElementById('current-driver-id').innerText = driverID;
    
    // Update UI active state
    document.querySelectorAll('#driver-list a').forEach(el => {
        el.classList.toggle('active', el.getAttribute('data-driver-id') === driverID);
    });

    // Fetch and render data
    const summary = await ApiService.getSummary();
    const driverInfo = summary.data.find(d => d.driverID === driverID);
    renderDriverSummaryInfo(driverInfo);
    
    await updateSpeedData(driverID);
}

function renderDriverSummaryInfo(driver) {
    if (!driver) return;
    const detailsElement = document.getElementById('driver-details');
    detailsElement.innerHTML = `
        <ul class="list-group list-group-flush">
            <li class="list-group-item d-flex justify-content-between"><span>Plate Number:</span> <strong>${driver.carPlateNumber}</strong></li>
            <li class="list-group-item d-flex justify-content-between"><span>Overspeeding Count:</span> <span class="badge bg-danger">${driver.totalOverspeedCount}</span></li>
            <li class="list-group-item d-flex justify-content-between"><span>Fatigue Alerts:</span> <span class="badge bg-warning text-dark">${driver.totalFatigueCount}</span></li>
            <li class="list-group-item d-flex justify-content-between"><span>Overspeed Time:</span> <span>${driver.totalOverspeedTimeSeconds}s</span></li>
            <li class="list-group-item d-flex justify-content-between"><span>Neutral Sliding:</span> <span>${driver.totalNeutralSlideTimeSeconds}s</span></li>
        </ul>
    `;
}

async function updateSpeedData(driverID) {
    const data = await ApiService.getSpeedData(driverID);
    if (data.status === 'success') {
        renderChart(data.speedData);
        updateAlerts(data.speedData, data.warning);
        document.getElementById('last-update').innerText = `Last update: ${new Date().toLocaleTimeString()}`;
    }
}

function renderChart(speedLines) {
    const ctx = document.getElementById('speedChart').getContext('2d');
    const labels = speedLines.map(d => d.time);
    const speeds = speedLines.map(d => d.speed);
    const colors = speedLines.map(d => d.isOverspeed ? 'rgba(255, 99, 132, 1)' : 'rgba(75, 192, 192, 1)');

    if (speedChart) {
        speedChart.destroy();
    }

    speedChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Vehicle Speed (km/h)',
                data: speeds,
                borderColor: 'rgba(75, 192, 192, 1)',
                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                segment: {
                    borderColor: ctx => speedLines[ctx.p0DataIndex].isOverspeed ? 'rgb(255, 99, 132)' : 'rgb(75, 192, 192)'
                },
                tension: 0.1,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'Speed (km/h)' }
                }
            },
            plugins: {
                legend: { position: 'top' }
            }
        }
    });
}

function updateAlerts(speedData, isWarning) {
    const alertsList = document.getElementById('alerts-list');
    const overspeeding = speedData.filter(d => d.isOverspeed === 1);
    
    if (isWarning || overspeeding.length > 0) {
        // Show red banner or intense warning
        const warningHTML = overspeeding.map(d => `
            <li class="list-group-item list-group-item-danger fw-bold">
                ⚠️ HIGH SPEED DETECTED: ${Math.round(d.speed)} km/h at ${d.time}
            </li>
        `).join('');
        alertsList.innerHTML = warningHTML || '<li class="list-group-item list-group-item-danger fw-bold">⚠️ DANGEROUS DRIVING BEHAVIOR DETECTED!</li>';
    } else {
        alertsList.innerHTML = '<li class="list-group-item list-group-item-success">Normal driving. No recent speed violations.</li>';
    }
}
