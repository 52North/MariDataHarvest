window.onload = () => {
    WAVE_VAR_LIST = ['VHM0_WW', 'VMDR_SW2', 'VMDR_SW1', 'VMDR', 'VTM10', 'VTPK', 'VPED',
        'VTM02', 'VMDR_WW', 'VTM01_SW2', 'VHM0_SW1',
        'VTM01_SW1', 'VSDX', 'VSDY', 'VHM0', 'VTM01_WW', 'VHM0_SW2']
    WIND_VAR_LIST = ['surface_downward_eastward_stress', 'wind_stress_divergence', 'northward_wind', 'sampling_length',
        'wind_speed_rms', 'wind_vector_curl',
        'northward_wind_rms', 'eastward_wind', 'wind_speed', 'wind_vector_divergence', 'wind_stress',
        'wind_stress_curl', 'eastward_wind_rms', 'surface_type',
        'surface_downward_northward_stress']

    DAILY_PHY_VAR_LIST = ['thetao', 'so', 'uo', 'vo', 'zos', 'mlotst', 'bottomT', 'siconc', 'sithick', 'usi', 'vsi']

    GFS_25_VAR_LIST = ['Temperature_surface', 'Wind_speed_gust_surface', 'u-component_of_wind_maximum_wind',
        'v-component_of_wind_maximum_wind', 'Dewpoint_temperature_height_above_ground',
        'U-Component_Storm_Motion_height_above_ground_layer',
        'V-Component_Storm_Motion_height_above_ground_layer', 'Relative_humidity_height_above_ground']

    createList(WAVE_VAR_LIST, 'Wave')
    createList(WIND_VAR_LIST, 'Wind')
    createList(DAILY_PHY_VAR_LIST, 'Physical')
    createList(GFS_25_VAR_LIST, 'GFS')
    let d = new Date();
    document.getElementById("date").innerHTML = d.toLocaleDateString('en-GB', {
        day: 'numeric', month: 'short', year: 'numeric'
    });
}

function createList(ls, name) {
    checkbox_list = document.getElementById('checkbox_list');
    let div = document.createElement("div")
    div.className = 'col w-75'
    let label = document.createElement("label")
    label.textContent = name + (name === 'GFS' ? '**' : '*');
    label.className = "pr-2 pl-2 envLabel firstEnvLabel " + name


    let btn = document.createElement("input")
    btn.type = 'checkbox'
    btn.className = "mt-1 mr-1 float-left"
    label.appendChild(btn);
    div.appendChild(label);
    div.appendChild(document.createElement("br"));


    checkbox_list.appendChild(document.createElement('br'));

    ls.forEach((variable) => {
        let checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.name = 'var-$$-' + name + '-$$-' + variable;
        checkbox.value = variable;
        checkbox.className = name + '_checkbox ' + "mr-2"

        let label = document.createElement('label')
        label.className = "col-auto envLabel " + name
        label.appendChild(checkbox);
        label.appendChild(document.createTextNode(variable));

        div.appendChild(label);
    });
    btn.onchange = (source) => {
        let checkboxes = document.getElementsByClassName(name + '_checkbox');
        [].forEach.call(checkboxes, cb => cb.checked = source.target.checked);
    }
    checkbox_list.appendChild(div);
    checkbox_list.appendChild(document.createElement('hr'));
}
