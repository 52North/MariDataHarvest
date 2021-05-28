window.onload = () => {
    $(function () {
      $('[data-toggle="tooltip"]').tooltip()
     })
    var mymap = L.map('mapid').setView([51.505, -0.09], 13);
    L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token=pk.eyJ1Ijoic3VmaWFuemEiLCJhIjoiY2twODloeGV5MDZweTJvbXBseWN2anc3ZiJ9.IoiJIA50gTlBv0nCXx1vVw', {
        attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
        maxZoom: 18,
        id: 'mapbox/streets-v11',
        tileSize: 512,
        zoomOffset: -1
    }).addTo(mymap);

    var bounds = [[$('#lat_lo').val(),$('#lon_lo').val()], [$('#lat_hi').val(), $('#lon_hi').val()]];
    var layerGroup = L.layerGroup().addTo(mymap)
    var boundingBox = L.rectangle(bounds, {color: "#ffffff", weight: 1}).addTo(layerGroup);
    mymap.fitBounds(bounds, {padding: [80,80]})
    var boundingBox_id = L.stamp(boundingBox);

    $('.bbox').on('input', (e)=>{
        layerGroup.removeLayer(boundingBox_id)
        var bounds = [[$('#lat_lo').val(),$('#lon_lo').val()], [$('#lat_hi').val(), $('#lon_hi').val()]];
        var boundingBox = L.rectangle(bounds, {color: "#ffffff", weight: 1}).addTo(layerGroup);
        mymap.fitBounds(bounds, {padding: [80,80]})
        mymap.addLayer(boundingBox);
        boundingBox_id= L.stamp(boundingBox);
    });

    WAVE_VAR_LIST = {'VHM0_WW':		'sea_surface_wind_wave_significant_height',
        'VMDR_SW2':		'sea_surface_secondary_swell_wave_from_direction',
        'VMDR_SW1':		'sea_surface_primary_swell_wave_from_direction',
        'VMDR':		'sea_surface_wave_from_direction',
        'VTM10':		'sea_surface_wave_mean_period_from_variance_spectral_density_inverse_frequency_moment',
        'VTPK':		'sea_surface_wave_period_at_variance_spectral_density_maximum',
        'VPED':		'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
        'VTM02':		'sea_surface_wave_mean_period_from_variance_spectral_density_second_frequency_moment',
        'VMDR_WW':		'sea_surface_wind_wave_from_direction',
        'VTM01_SW2':		'sea_surface_secondary_swell_wave_mean_period',
        'VHM0_SW1':		'sea_surface_primary_swell_wave_significant_height',
        'VTM01_SW1':		'sea_surface_primary_swell_wave_mean_period',
        'VSDX':		'sea_surface_wave_stokes_drift_x_velocity',
        'VSDY':		'sea_surface_wave_stokes_drift_y_velocity',
        'VHM0':		'sea_surface_wave_significant_height',
        'VTM01_WW':		'sea_surface_wind_wave_mean_period'}
        WIND_VAR_LIST = {'surface_downward_eastward_stress':'eastward wind stress',
        'wind_stress_divergence':'wind stress divergence',
        'northward_wind':'northward wind speed',
        'sampling_length':'sampling length',
        'wind_speed_rms':'wind speed root mean square',
        'wind_vector_curl':'wind vector curl',
        'northward_wind_rms':'northward wind speed root mean square',
        'eastward_wind':'eastward wind speed',
        'wind_speed':'wind speed',
        'wind_vector_divergence':'wind vector divergence',
        'wind_stress':'wind stress',
        'wind_stress_curl':'wind stress curl',
        'eastward_wind_rms':'eastward wind speed root mean square',
        'surface_type':'flag - 0:ocean - 1:earth/ice',
        'surface_downward_northward_stress':'northward wind stress'}

        DAILY_PHY_VAR_LIST = {'mlotst':'Density ocean mixed layer thickness',
        'siconc':'Ice concentration',
        'usi':'Sea ice eastward velocity',
        'thetao':'Potential Temperature',
        'sithick':'Sea ice thickness',
        'bottomT':'Sea floor potential temperature',
        'vsi':'Sea ice northward velocity',
        'vo':'Northward velocity',
        'uo':'Eastward velocity',
        'so':'Salinity',
        'zos':'Sea surface height'}
           GFS_25_VAR_LIST = {
        'Temperature_surface':'Temperature	Temperature @ Ground or water surface',
        'Wind_speed_gust_surface':'Wind speed (gust)	Wind speed (gust) @ Ground or water surface',
        'u-component_of_wind_maximum_wind':'u-component of wind	u-component of wind @ Maximum wind level',
        'v-component_of_wind_maximum_wind':'v-component of wind	v-component of wind @ Maximum wind level',
        'Dewpoint_temperature_height_above_ground':'Dewpoint temperature	Dewpoint temperature @ Specified height level above ground',
        'Relative_humidity_height_above_ground':'Relative humidity	Relative humidity @ Specified height level above ground',
        'U-Component_Storm_Motion_height_above_ground_layer':'U-Component Storm Motion	U-Component Storm Motion @ Specified height level above ground layer',
        'V-Component_Storm_Motion_height_above_ground_layer':'V-Component Storm Motion	V-Component Storm Motion @ Specified height level above ground layer'
        }

        createList(WAVE_VAR_LIST, 'Wave')
        createList(WIND_VAR_LIST, 'Wind')
        createList(DAILY_PHY_VAR_LIST, 'Physical')
        createList(GFS_25_VAR_LIST, 'GFS')

    let d = new Date();
    document.getElementById("date").innerHTML = d.toLocaleDateString('en-GB', {
        day: 'numeric', month: 'short', year: 'numeric'
    });

    document.getElementById('submitForm').onsubmit = ()=>{
        document.getElementById('submitBtn').disabled = true
        document.getElementById('spinnerPanel').hidden = false
    }

}

function createList(ls, name) {
    let checkbox_list = $('#checkbox_list');
    let div = $('<div class="col w-75"> </div>')
    let label = $('<label class="pr-2 pl-2 envLabel firstEnvLabel ' + name +'">'+ name + (name === 'GFS' ? '**' : '*') +'</label>')
    let btn = $('<input type="checkbox" class="mt-1 mr-1 float-left">')
    label.append(btn);
    div.append(label);
    let today = new Date();
    let tomorrow = new Date();

    if (name == 'Wind'){
    tomorrow.setDate(today.getDate()-4)
    div.append($('<i><small> Data provided til '+tomorrow.toLocaleDateString('en-GB', {
        day: 'numeric', month: 'short', year: 'numeric'
    }) +'<small></i>'));
    }else if(name === 'GFS'){
     tomorrow.setDate(today.getDate()+17)
    div.append($('<i><small> Data provided til '+tomorrow.toLocaleDateString('en-GB', {
        day: 'numeric', month: 'short', year: 'numeric'
    }) +'<small></i>'));
    }
    else{
    tomorrow.setDate(today.getDate()+9)
     div.append($('<i><small> Data provided til '+tomorrow.toLocaleDateString('en-GB', {
        day: 'numeric', month: 'short', year: 'numeric'
    }) +'<small></i>'));
    }
    div.append($("<br>"));
    for (let variable in ls){
        let checkbox = $('<input type="checkbox" class="'+ name + '_checkbox mr-2" name="var-$$-' + name + '-$$-' + variable+'" value="'+variable+'">')
        let label = $('<label class="col-auto envLabel ' + name +'" data-toggle="tooltip" data-placement="right" title="'+ ls[variable] +'"></label>')
        label.append(checkbox);
        label.append(variable);
        div.append(label);
    }
    btn.change((source) => {
        let checkboxes = document.getElementsByClassName(name + '_checkbox');
        [].forEach.call(checkboxes, cb => cb.checked = source.target.checked);
    });
    checkbox_list.append(div);
}
