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
    if (boundingBox_id !== -1)
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

    $('#removeFileBtn').click((event)=>{
        event.stopImmediatePropagation();
        event.preventDefault();
        $("#csvUpload").val('');
        $("#csvUpload").trigger('change')

    });

    $('#csvUpload').change(()=>{
        if (boundingBox_id !== -1){
            layerGroup.removeLayer(boundingBox_id)
            boundingBox_id = -1
        }
        var files = $('#csvUpload')[0].files;
        if(files.length > 0 ){
           $('#format_netcdf').prop('disabled',true);
           $('#format_csv').prop('checked',true);
           $('#boundingBoxPanel').hide()
           $('.uploadFileTools').attr("style", "display:block");
            var reader = new FileReader();
            reader.addEventListener('load', function (e) {
                var index = e.target.result.indexOf("\r\n");
                if (index === -1)
                  index= e.target.result.indexOf("\n");
                var header_list =e.target.result.substring(0, index).split(',');
                console.log(header_list);
                $('.select_header').each( (x, i) => {
                $(i).children().remove();
                header_list.forEach(head => {
                    $(i).append($('<option>'+head+'</option>').val(head));
                });
             });
            });
            reader.readAsBinaryString(files[0]);
           }else{
            $('#format_netcdf').prop('disabled',false);
            $('#boundingBoxPanel').show();
            $('#lat_lo').trigger('input');
            $('.uploadFileTools').attr("style", "display:none");
           }
    });

    $('#submitBtn').click((event)=>{
        var files = $('#csvUpload')[0].files;
        if(files.length > 0 ){
            cols = {
                'time':$('#timestamp_select').find(":selected").text(),
                'lat':$('#lat_select').find(":selected").text(),
                'lon':$('#lon_select').find(":selected").text()
            }
            event.stopImmediatePropagation();
            event.preventDefault();
            $('#submitBtn').prop('disabled',true);
            $('#spinnerPanel').prop('hidden',false);
            var fd = new FormData();
            vars = getVariables();
            fd.append('file',files[0]);
            fd.append('var',JSON.stringify(vars));
            fd.append('col',JSON.stringify(cols));

            $.ajax({
              url: window.location + '/merge_data',
              data: fd,
              processData: false,
              contentType: false,
              type: 'POST',
              success: function(data){
                $('html').html(data)
              }
            });
        }
    });

}

function getVariables() {
    data = []
    ls = ['Wave', 'Wind', 'GFS', 'Physical']
    ls.forEach( ds =>{
        $('.'+ds+'_checkbox').each( (x, i) => {
            if ($(i).is(':checked')){
             data.push(i.name)
            }
        });
    });
    return data
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
    tomorrow.setDate(today.getDate()-3)
    div.append($('<i><small> Data provided til '+tomorrow.toLocaleDateString('en-GB', {
        day: 'numeric', month: 'short', year: 'numeric'
    }) +'<small></i>'));
    }else if(name === 'GFS'){
     tomorrow.setDate(today.getDate()+15)
    div.append($('<i><small> Data provided til '+tomorrow.toLocaleDateString('en-GB', {
        day: 'numeric', month: 'short', year: 'numeric'
    }) +'<small></i>'));
    }
    else if(name === 'Wave' || name === 'Physical'){
    tomorrow.setDate(today.getDate()+8)
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
