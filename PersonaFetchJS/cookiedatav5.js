function createCookie(name, value, days) {
    var expires;

    if (days) {
        var date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        expires = "; expires=" + date.toGMTString();
    } else {
        expires = "";
    }
    document.cookie = encodeURIComponent(name) + "=" + encodeURIComponent(value) + expires + "; path=/";
}

function readCookie(name) {
    var nameEQ = encodeURIComponent(name) + "=";
    var ca = document.cookie.split(';');
    for (var i = 0; i < ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) === ' ') c = c.substring(1, c.length);
        if (c.indexOf(nameEQ) === 0) return decodeURIComponent(c.substring(nameEQ.length, c.length));
    }
    return null;
}


function httpGet(theUrl)
{
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", theUrl, false );
    xmlHttp.withCredentials = true;
    xmlHttp.send( null );
    return xmlHttp.responseText;
}



function getCookieData(callback) {
 
var userip = "";

var cookiedatav1 = readCookie('cookiedata');

if(typeof cookiedatav1 != 'undefined' && cookiedatav1){

}

 else{
 
  cookiedatav1 = httpGet("https://segmentsync.dc.cuberoot.co:8443/cookieProfile/getCookieData");
  createCookie('cookiedata',JSON.stringify(JSON.parse(cookiedatav1)),0.16);
 }

callback(cookiedatav1);

}



