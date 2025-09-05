/**
 * @file Logic chính cho trang Dashboard Analytics iCount People.
 * @description Quản lý trạng thái, gọi API, khởi tạo biểu đồ,
 * và cập nhật giao diện người dùng một cách linh hoạt.
 */
document.addEventListener('DOMContentLoaded', function () {
    // =========================================================================
    // STATE & CONFIGURATION
    // =========================================================================
    const API_BASE_URL = '/api/v1';
    let isInitialLoad = true; // Cờ để xử lý hiệu ứng tải trang lần đầu

    /**
     * @typedef {Object} Filters
     * @property {string} period - 'day', 'week', 'month', 'year'
     * @property {string} start_date - 'YYYY-MM-DD'
     * @property {string} end_date - 'YYYY-MM-DD'
     * @property {string} store - 'all' hoặc tên cửa hàng cụ thể
     */

    /**
     * @type {{tableData: Array<Object>, filters: Filters}}
     */
    const state = {
        tableData: [],
        filters: {
            period: 'month',
            start_date: '',
            end_date: '',
            store: 'all'
        }
    };

    // =========================================================================
    // DOM ELEMENT REFERENCES
    // =========================================================================
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
        summaryTotal: document.getElementById('summary-total'),
        summaryAverage: document.getElementById('summary-average'),
        latestTimestamp: document.getElementById('latest-data-timestamp'),
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

    // =========================================================================
    // CHART INSTANCES & CONFIGURATION
    // =========================================================================
    let trendChart, storeChart, datePickerInstance;

    const commonChartOptions = {
        chart: {
            toolbar: { show: true, tools: { download: true, selection: false, zoom: false, zoomin: false, zoomout: false, pan: false, reset: true }},
            foreColor: '#9ca3af',
            background: 'transparent'
        },
        grid: { borderColor: '#374151' },
        tooltip: { theme: 'dark' },
        noData: { text: 'Không có dữ liệu để hiển thị', style: { color: '#d1d5db' } }
    };

    const trendChartOptions = {
        ...commonChartOptions,
        series: [],
        chart: { ...commonChartOptions.chart, type: 'bar', height: 350 },
        plotOptions: { bar: { horizontal: false, columnWidth: '60%', borderRadius: 4 }},
        dataLabels: { enabled: false },
        stroke: { show: true, width: 2, colors: ['transparent'] },
        xaxis: { type: 'datetime', labels: { datetimeUTC: false, style: { colors: '#9ca3af' } }},
        yaxis: { title: { text: 'Lượt vào', style: { color: '#9ca3af' } }, labels: { style: { colors: '#9ca3af' } }},
    };

    const storeChartOptions = {
        ...commonChartOptions,
        series: [],
        chart: { ...commonChartOptions.chart, type: 'donut', height: 350 },
        labels: [],
        legend: { position: 'bottom', labels: { colors: '#d1d5db' } },
        dataLabels: { enabled: true, formatter: (val) => `${val.toFixed(1)}%` },
    };

    // =========================================================================
    // UTILITY FUNCTIONS
    // =========================================================================
    /**
     * Hiển thị hoặc ẩn lớp phủ loading.
     * @param {boolean} isLoading - Trạng thái loading.
     */
    const showLoading = (isLoading) => {
        if (isInitialLoad) return;
        elements.contentOverlay.classList.toggle('hidden', !isLoading);
        elements.contentOverlay.classList.toggle('flex', isLoading);
    };

    /**
     * Định dạng một số theo chuẩn Việt Nam.
     * @param {number} num - Số cần định dạng.
     * @returns {string} Chuỗi đã được định dạng.
     */
    const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(num);

    /**
     * Đóng/mở modal thông báo lỗi.
     * @param {boolean} show - True để hiển thị, false để ẩn.
     */
    const toggleModal = (show) => {
        if (show) {
            elements.error.modal.classList.remove('hidden', 'opacity-0');
            elements.error.modal.classList.add('flex');
            setTimeout(() => elements.error.modalPanel.classList.remove('scale-95', 'opacity-0'), 10);
        } else {
            elements.error.modalPanel.classList.add('scale-95', 'opacity-0');
            setTimeout(() => {
                elements.error.modal.classList.add('hidden', 'opacity-0');
                elements.error.modal.classList.remove('flex');
            }, 300);
        }
    };

    /**
     * Cập nhật URL của trình duyệt với các filter hiện tại mà không tải lại trang.
     */
    const updateURLWithFilters = () => {
        const params = new URLSearchParams(state.filters);
        window.history.pushState({}, '', `${window.location.pathname}?${params.toString()}`);
    }

    /**
     * Đọc các filter từ URL, cập nhật state và giao diện khi tải trang.
     */
    const applyFiltersFromURL = () => {
        const params = new URLSearchParams(window.location.search);
        state.filters.period = params.get('period') || 'month';
        state.filters.start_date = params.get('start_date') || '';
        state.filters.end_date = params.get('end_date') || '';
        state.filters.store = params.get('store') || 'all';

        elements.periodSelector.value = state.filters.period;
        // Việc cập nhật store selector và date picker sẽ diễn ra sau khi chúng được khởi tạo.
    }

    // =========================================================================
    // UI UPDATE FUNCTIONS
    // =========================================================================
    /**
     * Cập nhật các thẻ chỉ số KPI.
     * @param {object} metrics - Dữ liệu metrics từ API.
     */
    const updateMetrics = (metrics) => {
        elements.metrics.totalIn.textContent = formatNumber(metrics.total_in);
        elements.metrics.averageIn.textContent = formatNumber(metrics.average_in);
        elements.metrics.peakTime.textContent = metrics.peak_time || '--:--';
        elements.metrics.busiestStore.textContent = metrics.busiest_store || 'N/A';

        // Cập nhật thẻ tăng trưởng với màu sắc và icon tương ứng
        const { growth } = metrics;
        const growthEl = elements.metrics.growth;
        const growthCard = elements.metrics.growthCard;
        const iconContainer = growthCard.querySelector('[data-container="icon"]');
        const icon = iconContainer?.querySelector('[data-lucide]');
        if (!icon) return;

        growthEl.textContent = `${growth.toFixed(1)}%`;
        
        // Reset classes
        growthEl.className = 'text-4xl font-extrabold';
        iconContainer.className = 'p-2 rounded-lg';
        
        let iconName = 'minus', colorClass = 'gray';
        if (growth > 0) {
            colorClass = 'green';
            iconName = 'arrow-up-right';
        } else if (growth < 0) {
            colorClass = 'red';
            iconName = 'arrow-down-right';
        }

        growthEl.classList.add(`text-${colorClass}-400`);
        iconContainer.classList.add(`bg-${colorClass}-500/20`);
        icon.setAttribute('data-lucide', iconName);
        icon.className = `h-5 w-5 text-${colorClass}-400`;
        lucide.createIcons();
    };

    /**
     * Cập nhật dữ liệu cho 2 biểu đồ chính.
     * @param {object} trendData - Dữ liệu cho biểu đồ xu hướng.
     * @param {object} storeData - Dữ liệu cho biểu đồ tỷ trọng.
     */
    const updateCharts = (trendData, storeData) => {
        trendChart.updateSeries([{ name: 'Lượt vào', data: trendData.series }]);
        storeChart.updateOptions({
            series: storeData.series.map(p => p.y),
            labels: storeData.series.map(p => p.x)
        });
    };

    /**
     * Cập nhật bảng dữ liệu chi tiết và dòng tổng kết.
     * @param {object} tableData - Dữ liệu bảng từ API.
     */
    const updateTable = (tableData) => {
        state.tableData = tableData.data || [];
        if (state.tableData.length === 0) {
            elements.tableBody.innerHTML = `<tr><td colspan="4" class="text-center py-8 text-gray-400">Không có dữ liệu tổng hợp.</td></tr>`;
        } else {
            elements.tableBody.innerHTML = state.tableData.map(row => {
                const { period, total_in, proportion_pct, pct_change } = row;

                let changeClass = 'text-gray-300', changeIcon = 'minus', sign = '';
                if (pct_change > 0) {
                    changeClass = 'text-green-400';
                    changeIcon = 'trending-up';
                    sign = '+';
                } else if (pct_change < 0) {
                    changeClass = 'text-red-400';
                    changeIcon = 'trending-down';
                }

                return `
                    <tr class="hover:bg-gray-800 transition-colors duration-200">
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-300">${period}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-white font-semibold">${formatNumber(total_in)}</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-300">${proportion_pct.toFixed(2)}%</td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-semibold">
                            <div class="flex items-center ${changeClass}">
                                <i data-lucide="${changeIcon}" class="h-4 w-4 mr-1"></i>
                                <span>${sign}${pct_change.toFixed(1)}%</span>
                            </div>
                        </td>
                    </tr>`;
            }).join('');
        }
        // Cập nhật dòng tổng kết
        elements.summaryTotal.textContent = formatNumber(tableData.summary?.total_sum || 0);
        elements.summaryAverage.textContent = `TB: ${formatNumber(parseFloat(tableData.summary?.average_in || 0).toFixed(0))}`;
        lucide.createIcons();
    };

    /**
     * Cập nhật chuông thông báo lỗi.
     * @param {Array<object>} errorLogs - Danh sách log lỗi từ API.
     */
    const updateErrorNotifications = (errorLogs) => {
        const hasErrors = errorLogs && errorLogs.length > 0;
        elements.error.indicator.classList.toggle('hidden', !hasErrors);
        
        if (!hasErrors) {
            elements.error.logList.innerHTML = `<li class="text-gray-400">Không có lỗi nào được ghi nhận gần đây.</li>`;
            return;
        }

        elements.error.logList.innerHTML = errorLogs.map(log => `
            <li class="p-4 rounded-lg bg-gray-800/70 border border-gray-700">
                <div class="flex justify-between items-start">
                    <div>
                        <p class="font-bold text-red-400">${log.error_message}</p>
                        <p class="text-sm text-gray-400">Vị trí: <span class="font-medium text-gray-300">${log.store_name}</span> | Mã lỗi: ${log.error_code || 'N/A'}</p>
                    </div>
                    <p class="text-xs text-gray-500 whitespace-nowrap pl-4">${new Date(log.log_time).toLocaleString('vi-VN')}</p>
                </div>
            </li>`).join('');
    };

    /**
     * Cập nhật thời gian của dữ liệu mới nhất.
     * @param {string} timestamp - Chuỗi ISO timestamp.
     */
    const updateLatestTimestamp = (timestamp) => {
        if (elements.latestTimestamp && timestamp) {
            const formattedDate = new Date(timestamp).toLocaleString('vi-VN', {
                day: '2-digit', month: '2-digit', year: 'numeric',
                hour: '2-digit', minute: '2-digit'
            });
            elements.latestTimestamp.innerHTML = `Dữ liệu cập nhật lúc: <span class="font-semibold text-gray-300">${formattedDate}</span>`;
        }
    };

    // =========================================================================
    // DATA FETCHING
    // =========================================================================
    /**
     * Tải danh sách các cửa hàng và điền vào selector.
     */
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
            // Áp dụng lại filter store từ URL sau khi đã load xong
            if (Array.from(elements.storeSelector.options).some(opt => opt.value === state.filters.store)) {
                elements.storeSelector.value = state.filters.store;
            }
        } catch (error) {
            console.error('Error loading stores:', error);
        }
    }

    /**
     * Hàm chính để gọi API lấy dữ liệu dashboard và cập nhật UI.
     */
    async function fetchDashboardData() {
        showLoading(true);
        const params = new URLSearchParams(state.filters);
        try {
            const response = await fetch(`${API_BASE_URL}/dashboard?${params.toString()}`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            
            // Cập nhật UI
            if (isInitialLoad) {
                elements.skeletonLoader.classList.add('hidden');
                elements.dashboardContent.classList.remove('invisible');
                isInitialLoad = false;
            }
            updateMetrics(data.metrics);
            updateCharts(data.trend_chart, data.store_comparison_chart);
            updateTable(data.table_data);
            updateErrorNotifications(data.error_logs);
            updateLatestTimestamp(data.latest_record_time);
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

    // =========================================================================
    // EVENT HANDLERS & INITIALIZATION
    // =========================================================================
    /**
     * Khởi tạo các đối tượng biểu đồ ApexCharts.
     */
    function initCharts() {
        trendChart = new ApexCharts(document.querySelector('#trend-chart'), trendChartOptions);
        storeChart = new ApexCharts(document.querySelector('#store-chart'), storeChartOptions);
        trendChart.render();
        storeChart.render();
    }

    /**
     * Xử lý khi người dùng thay đổi lựa chọn "Xem theo".
     */
    const handlePeriodChange = () => {
        const period = elements.periodSelector.value;
        const today = new Date();
        let startDate, endDate = new Date(today);

        switch (period) {
            case 'day':
                startDate = new Date(today);
                break;
            case 'week':
                startDate = new Date(today.setDate(today.getDate() - today.getDay() + (today.getDay() === 0 ? -6 : 1)));
                endDate = new Date(new Date(startDate).setDate(startDate.getDate() + 6));
                break;
            case 'month':
                startDate = new Date(today.getFullYear(), today.getMonth(), 1);
                endDate = new Date(today.getFullYear(), today.getMonth() + 1, 0);
                break;
            case 'year':
                startDate = new Date(today.getFullYear(), 0, 1);
                endDate = new Date(today.getFullYear(), 11, 31);
                break;
        }
        if (datePickerInstance) {
            datePickerInstance.setDateRange(startDate, endDate);
        }
    };

    /**
     * Tải xuống dữ liệu trong bảng dưới dạng file CSV.
     */
    const downloadCsv = () => {
        if (state.tableData.length === 0) return alert('Không có dữ liệu để tải.');

        const headers = ['Ky bao cao', 'Tong luot vao', 'Ty trong (%)', 'Chenh lech (%)'];
        const csvRows = [
            headers.join(','),
            ...state.tableData.map(row =>
                [row.period, row.total_in, row.proportion_pct.toFixed(2), row.pct_change].join(',')
            )
        ];
        const csvString = csvRows.join('\n');
        const blob = new Blob(['\uFEFF' + csvString], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = `bao_cao_${state.filters.period}_${state.filters.start_date}_${state.filters.end_date}.csv`;
        link.click();
        URL.revokeObjectURL(link.href);
    };

    /**
     * Hàm khởi tạo chính của dashboard.
     */
    async function initializeDashboard() {
        applyFiltersFromURL(); // Đọc URL trước

        initCharts();

        // Khởi tạo date picker và set giá trị từ URL hoặc mặc định
        const today = new Date();
        const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

        const initialStartDate = state.filters.start_date ? new Date(state.filters.start_date) : firstDayOfMonth;
        const initialEndDate = state.filters.end_date ? new Date(state.filters.end_date) : today;

        // Thư viện Litepicker yêu cầu tham số cấu hình phải là camelCase
        datePickerInstance = new Litepicker({
            element: document.getElementById('date-range-picker'),
            singleMode: false,
            format: 'YYYY-MM-DD',
            startDate: initialStartDate,
            endDate: initialEndDate,
            setup: (picker) => picker.on('selected', (d1, d2) => {
                state.filters.start_date = d1.format('YYYY-MM-DD');
                state.filters.end_date = d2.format('YYYY-MM-DD');
            })
        });

        // Cập nhật state với giá trị date picker ban đầu
        state.filters.start_date = datePickerInstance.getStartDate().format('YYYY-MM-DD');
        state.filters.end_date = datePickerInstance.getEndDate().format('YYYY-MM-DD');

        // Gắn các event listener
        elements.applyFiltersBtn.addEventListener('click', () => {
            state.filters.period = elements.periodSelector.value;
            state.filters.store = elements.storeSelector.value;
            // Cập nhật lại ngày tháng từ picker phòng trường hợp người dùng chưa bấm apply
            state.filters.start_date = datePickerInstance.getStartDate().format('YYYY-MM-DD');
            state.filters.end_date = datePickerInstance.getEndDate().format('YYYY-MM-DD');
            updateURLWithFilters();
            fetchDashboardData();
        });
        elements.periodSelector.addEventListener('change', handlePeriodChange);
        elements.error.bell.addEventListener('click', () => toggleModal(true));
        elements.error.closeBtn.addEventListener('click', () => toggleModal(false));
        elements.downloadCsvBtn.addEventListener('click', downloadCsv);
        elements.sidebarToggleBtn.addEventListener('click', () => document.body.classList.toggle('sidebar-collapsed'));
        elements.error.modal.addEventListener('click', (e) => {
            if (e.target === elements.error.modal) toggleModal(false);
        });

        await loadStores();
        await fetchDashboardData();
        
        document.body.classList.add('sidebar-collapsed');
    }

    initializeDashboard();
});
