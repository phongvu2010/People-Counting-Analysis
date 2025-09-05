document.addEventListener('DOMContentLoaded', async function () {
    // --- STATE MANAGEMENT & CONSTANTS ---
    let isInitialLoad = true; // Cờ để kiểm tra lần tải đầu tiên

    const API_BASE_URL = '/api/v1';
    const state = {
        currentPage: 1,
        pageSize: 10,
        tableData: [],
        filters: { period: 'month', startDate: '', endDate: '', store: 'all' }
    };

    // --- DOM ELEMENTS ---
    const elements = {
        skeletonLoader: document.getElementById('skeleton-loader'),
        contentOverlay: document.getElementById('content-overlay'),
        dashboardContent: document.getElementById('dashboard-content'),
        applyFiltersBtn: document.getElementById('apply-filters-btn'),
        periodSelector: document.getElementById('period-selector'),
        storeSelector: document.getElementById('store-selector'),
        tableBody: document.getElementById('details-table-body'),
        downloadCsvBtn: document.getElementById('download-csv-btn'),
        sidebarToggleBtn: document.getElementById('sidebar-toggle-btn'),
        metrics: {
            totalIn: document.getElementById('metric-total-in'),
            averageIn: document.getElementById('metric-average-in'),
            peakTime: document.getElementById('metric-peak-time'),
            busiestStore: document.getElementById('metric-busiest-store'),
            growth: document.getElementById('metric-growth'),
            growthCard: document.getElementById('metric-growth-card'),
        },
        error: {
            indicator: document.getElementById('error-indicator'),
            bell: document.getElementById('notification-bell'),
            modal: document.getElementById('error-modal'),
            modalPanel: document.getElementById('error-modal-panel'),
            closeBtn: document.getElementById('close-error-modal-btn'),
            logList: document.getElementById('error-log-list'),
        }
    };

    // --- CHART & DATEPICKER INSTANCES ---
    let trendChart, storeChart, datePickerInstance;

    // --- CHART OPTIONS ---
    const commonChartOptions = {
        chart: {
            toolbar: {
                show: true,
                tools: {
                    download: true,
                    selection: false,
                    zoom: false,
                    zoomin: false,
                    zoomout: false,
                    pan: false,
                    reset: true
                }
            },
            foreColor: '#9ca3af'
        },
        grid: { borderColor: '#374151' },
        tooltip: { theme: 'dark' }
    };
    const trendChartOptions = {
        ...commonChartOptions,
        series: [],
        chart: { ...commonChartOptions.chart, type: 'bar', height: 350, background: 'transparent' },
        // Thêm các tùy chọn cho biểu đồ cột
        plotOptions: {
            bar: {
                horizontal: false,
                columnWidth: '60%', // Điều chỉnh độ rộng của các cột
                borderRadius: 4     // Bo tròn nhẹ các góc của cột cho đẹp mắt
            }
        },
        dataLabels: { enabled: false },
        stroke: { show: true, width: 2, colors: ['transparent'] },
        xaxis: {
            type: 'datetime',
            labels: { datetimeUTC: false, style: { colors: '#9ca3af' } }
        },
        yaxis: {
            title: { text: 'Lượt vào', style: { color: '#9ca3af' } },
            labels: { style: { colors: '#9ca3af' } }
        },
        fill: { opacity: 1 },
        noData: { text: 'Không có dữ liệu', style: { color: '#d1d5db' } }
    };
    const storeChartOptions = {
        ...commonChartOptions,
        series: [],
        chart: { ...commonChartOptions.chart, type: 'donut', height: 350, background: 'transparent' },
        labels: [],
        legend: { position: 'bottom', labels: { colors: '#d1d5db' } },
        dataLabels: { enabled: true, formatter: (val) => `${val.toFixed(1)}%` },
        noData: { ...trendChartOptions.noData }
    };

    // --- UTILITY FUNCTIONS ---
    const showLoading = (isLoading) => {
        if (isInitialLoad) return; // Lần đầu không làm gì, skeleton đã hiển thị sẵn
        elements.contentOverlay.classList.toggle('hidden', !isLoading);
        elements.contentOverlay.classList.toggle('flex', isLoading);
    };
    const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(num);
    const toggleModal = (show) => {
        if (show) {
            elements.error.modal.classList.remove('hidden');
            elements.error.modal.classList.add('flex');
            setTimeout(() => elements.error.modalPanel.classList.remove('scale-95', 'opacity-0'), 10);
        } else {
            elements.error.modalPanel.classList.add('scale-95', 'opacity-0');
            setTimeout(() => {
                elements.error.modal.classList.add('hidden');
                elements.error.modal.classList.remove('flex');
            }, 300);
        }
    };
    const debounce = (func, delay) => {
        let timeout;
        return function(...args) {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    // --- INITIALIZATION FUNCTIONS ---
    function initCharts() {
        trendChart = new ApexCharts(document.querySelector('#trend-chart'), trendChartOptions);
        storeChart = new ApexCharts(document.querySelector('#store-chart'), storeChartOptions);
        trendChart.render();
        storeChart.render();
    }

    function initDatePicker() {
        const today = new Date();
        const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

        state.filters.startDate = firstDayOfMonth.toISOString().split('T')[0];
        state.filters.endDate = today.toISOString().split('T')[0];

        datePickerInstance = new Litepicker({
            element: document.getElementById('date-range-picker'),
            singleMode: false,
            format: 'YYYY-MM-DD',
            startDate: firstDayOfMonth,
            endDate: today,
            setup: (picker) => picker.on('selected', (d1, d2) => {
                state.filters.startDate = d1.format('YYYY-MM-DD');
                state.filters.endDate = d2.format('YYYY-MM-DD');
            })
        });
    }

    function handlePeriodChange() {
        const period = elements.periodSelector.value;
        const today = new Date();

        let startDate = new Date(), endDate = new Date();

        switch (period) {
            case 'day': startDate = today; break;
            case 'week': startDate = new Date(today.setDate(today.getDate() - today.getDay() + (today.getDay() === 0 ? -6 : 1))); break;
            case 'month': startDate = new Date(today.getFullYear(), today.getMonth(), 1); break;
            case 'year': startDate = new Date(today.getFullYear(), 0, 1); break;
        }

        if (startDate && endDate && datePickerInstance) {
            datePickerInstance.setDateRange(startDate, endDate);
        }
    }

    function addEventListeners() {
        const debouncedFetch = debounce(() => {
            state.currentPage = 1;
            state.filters.period = elements.periodSelector.value;
            state.filters.store = elements.storeSelector.value;
            fetchDashboardData();
        }, 400);

        elements.applyFiltersBtn.addEventListener('click', debouncedFetch);
        elements.periodSelector.addEventListener('change', handlePeriodChange);
        elements.error.bell.addEventListener('click', () => toggleModal(true));
        elements.error.closeBtn.addEventListener('click', () => toggleModal(false));
        elements.downloadCsvBtn.addEventListener('click', downloadCsv);
        elements.error.modal.addEventListener('click', (e) => {
            if (e.target === elements.error.modal) toggleModal(false);
        });
        // Thêm event cho nút toggle
        elements.sidebarToggleBtn.addEventListener('click', () => document.body.classList.toggle('sidebar-collapsed'));
    }

    function downloadCsv() {
        if (state.tableData.length === 0) {
            alert('Không có dữ liệu để tải.');
            return;
        }

        const headers = ['Kỳ báo cáo', 'Tổng lượt vào', 'Chênh lệch (%)'];

        // Tạo các hàng dữ liệu cho file CSV
        const csvRows = [
        headers.join(','), // Hàng tiêu đề
        ...state.tableData.map(row => [row.period, row.total_in, row.pct_change].join(','))];

        // Tạo chuỗi CSV hoàn chỉnh với ký tự xuống dòng
        const csvString = csvRows.join('\n');

        // Thêm BOM để Excel đọc tiếng Việt có dấu đúng
        const blob = new Blob(['\uFEFF' + csvString], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement("a");

        link.setAttribute('href', URL.createObjectURL(blob));
        link.setAttribute('download', `bao_cao_tong_hop_${new Date().toISOString().split('T')[0]}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    // --- DATA FETCHING & UI UPDATING ---
    async function loadStores() {
        try {
            const response = await fetch(`${API_BASE_URL}/stores`);
            if (!response.ok) throw new Error('Failed to load stores');
            const stores = await response.json();
            stores.forEach(store => {
                const option = document.createElement('option');
                option.value = store;
                option.textContent = store;
                elements.storeSelector.appendChild(option);
            });
        } catch (error) {
            console.error('Error loading stores:', error);
        }
    }

    async function fetchDashboardData() {
        showLoading(true);

        const params = new URLSearchParams({
            period: state.filters.period,
            start_date: state.filters.startDate,
            end_date: state.filters.endDate,
            store: state.filters.store,
        });
        const url = `${API_BASE_URL}/dashboard?${params.toString()}`;

        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            updateUI(data);
        } catch (error) {
            console.error('Failed to fetch dashboard data:', error);
            if (isInitialLoad) {
                elements.skeletonLoader.classList.add('hidden');
                elements.dashboardContent.classList.remove('invisible');
            }
            elements.tableBody.innerHTML = `<tr><td colspan="4" class="text-center py-8 text-red-400">Tải dữ liệu thất bại. Vui lòng thử lại.</td></tr>`;
        } finally {
            showLoading(false);
        }
    }

    function updateUI(data) {
        if (isInitialLoad) {
            elements.skeletonLoader.classList.add('hidden');
            elements.dashboardContent.classList.remove('invisible');
            isInitialLoad = false;
        }

        updateMetrics(data.metrics);
        updateCharts(data.trend_chart, data.store_comparison_chart);
        state.tableData = data.table_data.data;
        updateTable(data.table_data);
        updateSummaryRow(data.table_data.summary);
        updateErrorNotifications(data.error_logs);
        updateLatestTimestamp(data.latest_record_time);
    }

    function updateLatestTimestamp(timestamp) {
        const timestampEl = document.getElementById('latest-data-timestamp');

        if (timestampEl && timestamp) {
            // Định dạng lại ngày giờ theo kiểu Việt Nam
            const formattedDate = new Date(timestamp).toLocaleString('vi-VN', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            }).replace(',', '');

            timestampEl.innerHTML = `Dữ liệu gần nhất: <br> <span class="font-semibold text-gray-400">${formattedDate}</span>`;
        } else if (timestampEl) {
            timestampEl.textContent = 'Dữ liệu gần nhất: Không rõ';
        }
    }

    function updateMetrics(metrics) {
        elements.metrics.totalIn.textContent = formatNumber(metrics.total_in);
        elements.metrics.averageIn.textContent = formatNumber(metrics.average_in);
        elements.metrics.peakTime.textContent = metrics.peak_time || '--:--';
        elements.metrics.busiestStore.textContent = metrics.busiest_store || 'N/A';

        const growthValue = metrics.growth;
        const growthEl = elements.metrics.growth;
        const growthCard = elements.metrics.growthCard;
        const growthIconDiv = growthCard.querySelector('[data-container="icon"]');

        const growthIcon = growthIconDiv ? growthIconDiv.querySelector('[data-lucide]') : null;
        if (!growthIcon) return;
        growthEl.textContent = `${growthValue.toFixed(1)}%`;
        ['text-green-400', 'text-red-400', 'text-white'].forEach(c => growthEl.classList.remove(c));
        ['hover:shadow-green-500/20', 'hover:border-green-500/50', 'hover:shadow-red-500/20', 'hover:border-red-500/50'].forEach(c => growthCard.classList.remove(c));
        ['bg-green-500/20', 'bg-red-500/20', 'bg-gray-500/20'].forEach(c => growthIconDiv.classList.remove(c));

        let iconName = 'arrow-right', iconColor = 'text-gray-400';
        if (growthValue > 0) {
            growthEl.classList.add('text-green-400');
            growthCard.classList.add('hover:shadow-green-500/20', 'hover:border-green-500/50');
            growthIconDiv.classList.add('bg-green-500/20');
            iconName = 'arrow-up-right';
            iconColor = 'text-green-400';
        } else if (growthValue < 0) {
            growthEl.classList.add('text-red-400');
            growthCard.classList.add('hover:shadow-red-500/20', 'hover:border-red-500/50');
            growthIconDiv.classList.add('bg-red-500/20');
            iconName = 'arrow-down-right';
            iconColor = 'text-red-400';
        } else {
            growthEl.classList.add('text-white');
            growthIconDiv.classList.add('bg-gray-500/20');
        }

        growthIcon.setAttribute('data-lucide', iconName);
        growthIcon.className = `h-5 w-5 ${iconColor}`;
        lucide.createIcons();
    }

    function updateCharts(trendData, storeData) {
        trendChart.updateSeries([{
            name: 'Lượt vào',
            data: trendData.series.map(p => ({ x: p.x, y: p.y }))
        }]);
        storeChart.updateOptions({
            series: storeData.series.map(p => p.y),
            labels: storeData.series.map(p => p.x)
        });
    }

    function updateTable(tableData) {
        if (!tableData.data || tableData.data.length === 0) {
            elements.tableBody.innerHTML = `<tr><td colspan="3" class="text-center py-8 text-gray-400">Không có dữ liệu tổng hợp.</td></tr>`;
            return;
        }

        elements.tableBody.innerHTML = tableData.data.map(row => {
            const pct_change = row.pct_change;
            let changeClass = 'text-gray-300', changeIcon = '<i data-lucide="minus" class="h-4 w-4 mr-1"></i>', sign = pct_change > 0 ? '+' : '';
            if (pct_change > 0) {
                changeClass = 'text-green-400';
                changeIcon = '<i data-lucide="trending-up" class="h-4 w-4 mr-1"></i>';
            } else if (pct_change < 0) {
                changeClass = 'text-red-400';
                changeIcon = '<i data-lucide="trending-down" class="h-4 w-4 mr-1"></i>';
            }

            return `<tr class="hover:bg-gray-800 transition-colors duration-200">
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-300">${row.period}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-white font-semibold">${formatNumber(row.total_in)}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-semibold"><div class="flex items-center ${changeClass}">${changeIcon}<span>${sign}${pct_change.toFixed(1)}%</span></div></td>
                </tr>`;
        }).join('');
        lucide.createIcons();
    }

    function updateSummaryRow(summary) {
        const summaryTotalEl = document.getElementById('summary-total');
        const summaryAverageEl = document.getElementById('summary-average');

        if (summary && summaryTotalEl && summaryAverageEl) {
            summaryTotalEl.textContent = formatNumber(summary.total_sum || 0);
            summaryAverageEl.textContent = `TB: ${formatNumber(parseFloat(summary.average_in || 0).toFixed(1))}`;
        }
    }

    function updateErrorNotifications(errorLogs) {
        elements.error.indicator.classList.toggle('hidden', !errorLogs || errorLogs.length === 0);
        if (!errorLogs || errorLogs.length === 0) {
            elements.error.logList.innerHTML = `<li class="text-gray-400">Không có lỗi nào được ghi nhận gần đây.</li>`;
            return;
        }

        elements.error.logList.innerHTML = errorLogs.map(log => `
            <li class="p-4 rounded-lg bg-gray-800/70 border border-gray-700">
                <div class="flex justify-between items-start">
                    <div>
                        <p class="font-bold text-red-400">${log.error_message}</p>
                        <p class="text-sm text-gray-400">Vị trí: <span class="font-medium text-gray-300">${log.store_name}</span> | Mã lỗi: ${log.error_code}</p>
                    </div>
                    <p class="text-xs text-gray-500 whitespace-nowrap pl-4">${new Date(log.log_time).toLocaleString('vi-VN')}</p>
                </div>
            </li>`).join('');
    }

    // --- SEQUENTIAL INITIALIZATION ---
    try {
        initCharts();
        initDatePicker();
        addEventListeners();
        document.body.classList.add('sidebar-collapsed');

        await loadStores();
        await fetchDashboardData();
    } catch (error) {
        console.error('An error occurred during initial page load:', error);
    }
});
