We can target users using DFP key value pair targeting.
We can also use boolean operations on different key values for campaign targeting as shown in images.
This method is free of charge, real time and no segment syncing is required and client also don't need to contact their Google Account Manager.
We have already done the campaign for wittyfeed using key value pair targeting and it scaled well.
Please find the documentation attached which details how to set it up in DFP.
Also, if I could please get our DFP account credentials.
First, three images show setting up key values in DFP for different campaigns.
4th image shows --> passing user persona to DFP, stored in cookie via our javascript. As one can see key value pairs being passed in gampad ad call in 4th image.

Some of documents obtained from google -
https://support.google.com/admanager/answer/177381


 getCookieData(function(cookiedatav1) {
 
    var cubecity;
    var cubemobile;
    var cubetags;
    var cubeinmarket;
    var cubeaffinity;
    var cubeage;
    var cubegender;
    var cubeincomelevel;
    var cubesection;

     cubecity       = JSON.parse(cookiedatav1).city;
     cubemobile     = JSON.parse(cookiedatav1).mobileDevice;
     cubetags       = JSON.parse(cookiedatav1).tags;
     cubeinmarket   = JSON.parse(cookiedatav1).inMarketSegments;
     cubeaffinity   = JSON.parse(cookiedatav1).AffinitySegments;
     cubeage        = JSON.parse(cookiedatav1).age;
     cubegender     = JSON.parse(cookiedatav1).gender;
     cubeincomelevel= JSON.parse(cookiedatav1).incomelevel;
     cubesection    = JSON.parse(cookiedatav1).section;

     window.cubeRootTargetingSlot = [
       [['cinma', cubeinmarket], ['caffin', cubeaffinity],['ccity', cubecity],['cmobile',cubemobile], ['ctags',cubetags],['cgen',cubegender], ['cag', cubeage],['cinc', cubeincomelevel],['csection', cubesection]] ];
   });