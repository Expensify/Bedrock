#include "DB.h"

#include <string.h>

#include <BedrockServer.h>
#include <libstuff/SQResult.h>

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

const string test_policy = "{\"approvalMode\":\"OPTIONAL\",\"approver\":\"test_user_userx1699552996668.6@testuser.testify.com\",\"autoReporting\":true,\"automaticJoiningEnabled\":true,\"categoryObjects\":"
"{\"Advertising\":{\"enabled\":true,\"name\":\"Advertising\",\"areCommentsRequired\":false},\"Benefits\":{\"enabled\":true,\"name\":\"Benefits\",\"areCommentsRequired\":false},"
"\"Car\":{\"enabled\":true,\"name\":\"Car\",\"areCommentsRequired\":false},\"Equipment\":{\"enabled\":true,\"name\":\"Equipment\",\"areCommentsRequired\":false},\"Fees\":"
"{\"enabled\":true,\"name\":\"Fees\",\"areCommentsRequired\":false},\"Home Office\":{\"enabled\":true,\"name\":\"Home Office\",\"areCommentsRequired\":false},\"Insurance\":"
"{\"enabled\":true,\"name\":\"Insurance\",\"areCommentsRequired\":false},\"Interest\":{\"enabled\":true,\"name\":\"Interest\",\"areCommentsRequired\":false},\"Labor\":"
"{\"enabled\":true,\"name\":\"Labor\",\"areCommentsRequired\":false},\"Maintenance\":{\"enabled\":true,\"name\":\"Maintenance\",\"areCommentsRequired\":false},\"Materials\":"
"{\"enabled\":true,\"name\":\"Materials\",\"areCommentsRequired\":false},\"Meals and Entertainment\":{\"enabled\":true,\"name\":\"Meals and Entertainment\",\"areCommentsRequired\":false},"
"\"Office Supplies\":{\"enabled\":true,\"name\":\"Office Supplies\",\"areCommentsRequired\":false},\"Other\":{\"enabled\":true,\"name\":\"Other\",\"areCommentsRequired\":false},\"Professional Services\":"
"{\"enabled\":true,\"name\":\"Professional Services\",\"areCommentsRequired\":false},\"Rent\":{\"enabled\":true,\"name\":\"Rent\",\"areCommentsRequired\":false},\"Taxes\":{\"enabled\":true,\"name\":"
"\"Taxes\",\"areCommentsRequired\":false},\"Travel\":{\"enabled\":true,\"name\":\"Travel\",\"areCommentsRequired\":false},\"Utilities\":{\"enabled\":true,\"name\":\"Utilities\",\"areCommentsRequired\":false}},"
"\"customUnits\":[{\"attributes\":{\"unit\":\"mi\"},\"customUnitID\":\"654d1ee5302e5\",\"defaultCategory\":\"Car\",\"enabled\":true,\"name\":\"Distance\",\"rates\":[{\"currency\":\"USD\","
"\"customUnitRateID\":\"654d1ee5302e7\",\"enabled\":true,\"name\":\"Default Rate\",\"rate\":65.5}]}],\"defaultBillable\":false,\"defaultCategoryOrigin\":\"US\",\"defaultReimbursable\":true,\"disabledFields\":"
"{\"defaultBillable\":true,\"reimbursable\":false},\"eReceipts\":true,\"employeeList\":[{\"email\":\"test_user_userx1699552996668.6@testuser.testify.com\",\"forwardsTo\":\"\",\"role\":\"admin\",\"submitsTo\":"
"\"test_user_userx1699552996668.6@testuser.testify.com\"},{\"email\":\"test_user_usery1699552997227@testuser.testify.com\",\"role\":\"admin\",\"submitsTo\":\"test_user_userx1699552996668.6@testuser.testify.com\"}],"
"\"exportLayouts\":[],\"fcrmEnabled\":false,\"fieldList\":[{\"defaultExternalID\":null,\"defaultValue\":\"{report:type} {report:startdate}\",\"deletable\":true,\"disabledOptions\":[],\"externalID\":null,"
"\"externalIDs\":[],\"fieldID\":\"text_title\",\"isTax\":false,\"keys\":[],\"name\":\"title\",\"orderWeight\":0,\"origin\":null,\"target\":\"expense\",\"type\":\"formula\",\"value\":null,\"values\":[]}],"
"\"glCodes\":true,\"harvesting\":{\"enabled\":true},\"invoice\":{\"markUp\":0},\"isReceiptVisibilityPublic\":false,\"lastModified\":1699552997976593,\"maxExpenseAge\":10000000000,"
"\"maxExpenseAmount\":10000000000,\"maxExpenseAmountNoReceipt\":10000000000,\"mccGroup\":{\"airlines\":{\"category\":\"Travel\",\"groupID\":\"airlines\"},\"commuter\":{\"category\":\"Car\","
"\"groupID\":\"commuter\"},\"gas\":{\"category\":\"Car\",\"groupID\":\"gas\"},\"goods\":{\"category\":\"Materials\",\"groupID\":\"goods\"},\"groceries\":{\"category\":\"Meals and Entertainment\","
"\"groupID\":\"groceries\"},\"hotel\":{\"category\":\"Travel\",\"groupID\":\"hotel\"},\"mail\":{\"category\":\"Office Supplies\",\"groupID\":\"mail\"},\"meals\":{\"category\":\"Meals and Entertainment\","
"\"groupID\":\"meals\"},\"rental\":{\"category\":\"Travel\",\"groupID\":\"rental\"},\"services\":{\"category\":\"Professional Services\",\"groupID\":\"services\"},\"taxi\":{\"category\":\"Travel\",\"groupID\":"
"\"taxi\"},\"uncategorized\":{\"category\":\"Other\",\"groupID\":\"uncategorized\"},\"utilities\":{\"category\":\"Utilities\",\"groupID\":\"utilities\"}},\"mccToCategory\":{\"mcc1520\":\"Professional Services\","
"\"mcc1711\":\"Professional Services\",\"mcc1731\":\"Professional Services\",\"mcc1740\":\"Professional Services\",\"mcc1750\":\"Professional Services\",\"mcc1761\":\"Professional Services\",\"mcc1771\":"
"\"Professional Services\",\"mcc1799\":\"Professional Services\",\"mcc2741\":\"Professional Services\",\"mcc2791\": \"Professional Services\",\"mcc2842\":\"Professional Services\",\"mcc3000\":\"Travel\","
"\"mcc3001\":\"Travel\",\"mcc3002\":\"Travel\",\"mcc3003\":\"Travel\",\"mcc3004\":\"Travel\",\"mcc3005\":\"Travel\",\"mcc3006\":\"Travel\",\"mcc3007\":\"Travel\",\"mcc3008\":\"Travel\",\"mcc3009\":\"Travel\","
"\"mcc3010\":\"Travel\",\"mcc3011\":\"Travel\",\"mcc3012\":\"Travel\",\"mcc3013\":\"Travel\",\"mcc3014\":\"Travel\",\"mcc3015\":\"Travel\",\"mcc3016\":\"Travel\",\"mcc3017\":\"Travel\",\"mcc3018\":\"Travel\","
"\"mcc3019\":\"Travel\",\"mcc3020\":\"Travel\",\"mcc3021\":\"Travel\",\"mcc3022\":\"Travel\",\"mcc3023\":\"Travel\",\"mcc3024\":\"Travel\",\"mcc3025\":\"Travel\",\"mcc3026\":\"Travel\",\"mcc3027\":\"Travel\","
"\"mcc3028\":\"Travel\",\"mcc3029\":\"Travel\",\"mcc3030\":\"Travel\",\"mcc3031\":\"Travel\",\"mcc3032\":\"Travel\",\"mcc3033\":\"Travel\",\"mcc3034\":\"Travel\",\"mcc3035\":\"Travel\",\"mcc3036\":\"Travel\","
"\"mcc3037\":\"Travel\",\"mcc3038\":\"Travel\",\"mcc3039\":\"Travel\",\"mcc3040\":\"Travel\",\"mcc3041\":\"Travel\",\"mcc3042\":\"Travel\",\"mcc3043\":\"Travel\",\"mcc3044\":\"Travel\",\"mcc3045\":\"Travel\","
"\"mcc3046\":\"Travel\",\"mcc3047\":\"Travel\",\"mcc3048\":\"Travel\",\"mcc3049\":\"Travel\",\"mcc3050\":\"Travel\",\"mcc3051\":\"Travel\",\"mcc3052\":\"Travel\",\"mcc3053\":\"Travel\",\"mcc3054\":\"Travel\","
"\"mcc3055\":\"Travel\",\"mcc3056\":\"Travel\",\"mcc3057\":\"Travel\",\"mcc3058\":\"Travel\",\"mcc3059\":\"Travel\",\"mcc3060\":\"Travel\",\"mcc3061\":\"Travel\",\"mcc3062\":\"Travel\",\"mcc3063\":\"Travel\","
"\"mcc3064\":\"Travel\",\"mcc3065\":\"Travel\",\"mcc3066\":\"Travel\",\"mcc3067\":\"Travel\",\"mcc3068\":\"Travel\",\"mcc3069\":\"Travel\",\"mcc3070\":\"Travel\",\"mcc3071\":\"Travel\",\"mcc3072\":\"Travel\","
"\"mcc3073\":\"Travel\",\"mcc3074\":\"Travel\",\"mcc3075\":\"Travel\",\"mcc3076\":\"Travel\",\"mcc3077\":\"Travel\",\"mcc3078\":\"Travel\",\"mcc3079\":\"Travel\",\"mcc3080\":\"Travel\",\"mcc3081\":\"Travel\","
"\"mcc3082\":\"Travel\",\"mcc3083\":\"Travel\",\"mcc3084\":\"Travel\",\"mcc3085\":\"Travel\",\"mcc3086\":\"Travel\",\"mcc3087\":\"Travel\",\"mcc3088\":\"Travel\",\"mcc3089\":\"Travel\",\"mcc3090\":\"Travel\","
"\"mcc3091\":\"Travel\",\"mcc3092\":\"Travel\",\"mcc3093\":\"Travel\",\"mcc3094\":\"Travel\",\"mcc3095\":\"Travel\",\"mcc3096\":\"Travel\",\"mcc3097\":\"Travel\",\"mcc3098\":\"Travel\",\"mcc3099\":\"Travel\","
"\"mcc3100\":\"Travel\",\"mcc3101\":\"Travel\",\"mcc3102\":\"Travel\",\"mcc3103\":\"Travel\",\"mcc3104\":\"Travel\",\"mcc3105\":\"Travel\",\"mcc3106\":\"Travel\",\"mcc3107\":\"Travel\",\"mcc3108\":\"Travel\","
"\"mcc3109\":\"Travel\",\"mcc3110\":\"Travel\",\"mcc3111\":\"Travel\",\"mcc3112\":\"Travel\",\"mcc3113\":\"Travel\",\"mcc3114\":\"Travel\",\"mcc3115\":\"Travel\",\"mcc3116\":\"Travel\",\"mcc3117\":\"Travel\","
"\"mcc3118\":\"Travel\",\"mcc3119\":\"Travel\",\"mcc3120\":\"Travel\",\"mcc3121\":\"Travel\",\"mcc3122\":\"Travel\",\"mcc3123\":\"Travel\",\"mcc3124\":\"Travel\",\"mcc3125\":\"Travel\",\"mcc3126\":\"Travel\","
"\"mcc3127\":\"Travel\",\"mcc3128\":\"Travel\",\"mcc3129\":\"Travel\",\"mcc3130\":\"Travel\",\"mcc3131\":\"Travel\",\"mcc3132\":\"Travel\",\"mcc3133\":\"Travel\",\"mcc3134\":\"Travel\",\"mcc3135\":\"Travel\","
"\"mcc3136\":\"Travel\",\"mcc3137\":\"Travel\",\"mcc3138\":\"Travel\",\"mcc3139\":\"Travel\",\"mcc3140\":\"Travel\",\"mcc3141\":\"Travel\",\"mcc3142\":\"Travel\",\"mcc3143\":\"Travel\",\"mcc3144\":\"Travel\","
"\"mcc3145\":\"Travel\",\"mcc3146\":\"Travel\",\"mcc3147\":\"Travel\",\"mcc3148\":\"Travel\",\"mcc3149\":\"Travel\",\"mcc3150\":\"Travel\",\"mcc3151\":\"Travel\",\"mcc3152\":\"Travel\",\"mcc3153\":\"Travel\","
"\"mcc3154\":\"Travel\",\"mcc3155\":\"Travel\",\"mcc3156\":\"Travel\",\"mcc3157\":\"Travel\",\"mcc3158\":\"Travel\",\"mcc3159\":\"Travel\",\"mcc3160\":\"Travel\",\"mcc3161\":\"Travel\",\"mcc3162\":\"Travel\","
"\"mcc3163\":\"Travel\",\"mcc3164\":\"Travel\",\"mcc3165\":\"Travel\",\"mcc3166\":\"Travel\",\"mcc3167\":\"Travel\",\"mcc3168\":\"Travel\",\"mcc3169\":\"Travel\",\"mcc3170\":\"Travel\",\"mcc3171\":\"Travel\","
"\"mcc3172\":\"Travel\",\"mcc3173\":\"Travel\",\"mcc3174\":\"Travel\",\"mcc3175\":\"Travel\",\"mcc3176\":\"Travel\",\"mcc3177\":\"Travel\",\"mcc3178\":\"Travel\",\"mcc3179\":\"Travel\",\"mcc3180\":\"Travel\","
"\"mcc3181\":\"Travel\",\"mcc3182\":\"Travel\",\"mcc3183\":\"Travel\",\"mcc3184\":\"Travel\",\"mcc3185\":\"Travel\",\"mcc3186\":\"Travel\",\"mcc3187\":\"Travel\",\"mcc3188\":\"Travel\",\"mcc3189\":\"Travel\","
"\"mcc3190\":\"Travel\",\"mcc3191\":\"Travel\",\"mcc3192\":\"Travel\",\"mcc3193\":\"Travel\",\"mcc3194\":\"Travel\","
"\"mcc3195\":\"Travel\",\"mcc3196\":\"Travel\",\"mcc3197\":\"Travel\",\"mcc3198\":\"Travel\",\"mcc3199\":\"Travel\",\"mcc3200\":\"Travel\",\"mcc3201\":\"Travel\",\"mcc3202\":\"Travel\",\"mcc3203\":\"Travel\","
"\"mcc3204\":\"Travel\",\"mcc3205\":\"Travel\",\"mcc3206\":\"Travel\",\"mcc3207\":\"Travel\",\"mcc3208\":\"Travel\",\"mcc3209\":\"Travel\",\"mcc3210\":\"Travel\",\"mcc3211\":\"Travel\",\"mcc3212\":\"Travel\","
"\"mcc3213\":\"Travel\",\"mcc3214\":\"Travel\",\"mcc3215\":\"Travel\",\"mcc3216\":\"Travel\",\"mcc3217\":\"Travel\",\"mcc3218\":\"Travel\",\"mcc3219\":\"Travel\",\"mcc3220\":\"Travel\",\"mcc3221\":\"Travel\","
"\"mcc3222\":\"Travel\",\"mcc3223\":\"Travel\",\"mcc3224\":\"Travel\",\"mcc3225\":\"Travel\",\"mcc3226\":\"Travel\",\"mcc3227\":\"Travel\",\"mcc3228\":\"Travel\",\"mcc3229\":\"Travel\",\"mcc3230\":\"Travel\","
"\"mcc3231\":\"Travel\",\"mcc3232\":\"Travel\",\"mcc3233\":\"Travel\",\"mcc3234\":\"Travel\",\"mcc3235\":\"Travel\",\"mcc3236\":\"Travel\",\"mcc3237\":\"Travel\",\"mcc3238\":\"Travel\",\"mcc3239\":\"Travel\","
"\"mcc3240\":\"Travel\",\"mcc3241\":\"Travel\",\"mcc3242\":\"Travel\",\"mcc3243\":\"Travel\",\"mcc3244\":\"Travel\",\"mcc3245\":\"Travel\",\"mcc3246\":\"Travel\",\"mcc3247\":\"Travel\",\"mcc3248\":\"Travel\","
"\"mcc3249\":\"Travel\",\"mcc3250\":\"Travel\",\"mcc3251\":\"Travel\",\"mcc3252\":\"Travel\",\"mcc3253\":\"Travel\",\"mcc3254\":\"Travel\",\"mcc3255\":\"Travel\",\"mcc3256\":\"Travel\",\"mcc3257\":\"Travel\","
"\"mcc3258\":\"Travel\",\"mcc3259\":\"Travel\",\"mcc3260\":\"Travel\",\"mcc3261\":\"Travel\",\"mcc3262\":\"Travel\",\"mcc3263\":\"Travel\",\"mcc3264\":\"Travel\",\"mcc3265\":\"Travel\",\"mcc3266\":\"Travel\","
"\"mcc3267\":\"Travel\",\"mcc3268\":\"Travel\",\"mcc3269\":\"Travel\",\"mcc3270\":\"Travel\",\"mcc3271\":\"Travel\",\"mcc3272\":\"Travel\",\"mcc3273\":\"Travel\",\"mcc3274\":\"Travel\",\"mcc3275\":\"Travel\","
"\"mcc3276\":\"Travel\",\"mcc3277\":\"Travel\",\"mcc3278\":\"Travel\",\"mcc3279\":\"Travel\",\"mcc3280\":\"Travel\",\"mcc3281\":\"Travel\",\"mcc3282\":\"Travel\",\"mcc3283\":\"Travel\",\"mcc3284\":\"Travel\","
"\"mcc3285\":\"Travel\",\"mcc3286\":\"Travel\",\"mcc3287\":\"Travel\",\"mcc3288\":\"Travel\",\"mcc3289\":\"Travel\",\"mcc3290\":\"Travel\",\"mcc3291\":\"Travel\",\"mcc3292\":\"Travel\",\"mcc3293\":\"Travel\","
"\"mcc3294\":\"Travel\",\"mcc3295\":\"Travel\",\"mcc3296\":\"Travel\",\"mcc3297\":\"Travel\",\"mcc3298\":\"Travel\",\"mcc3299\":\"Travel\",\"mcc3351\":\"Travel\",\"mcc3352\":\"Travel\",\"mcc3353\":\"Travel\","
"\"mcc3354\":\"Travel\",\"mcc3355\":\"Travel\",\"mcc3356\":\"Travel\",\"mcc3357\":\"Travel\",\"mcc3358\":\"Travel\",\"mcc3359\":\"Travel\",\"mcc3360\":\"Travel\",\"mcc3361\":\"Travel\",\"mcc3362\":\"Travel\","
"\"mcc3363\":\"Travel\",\"mcc3364\":\"Travel\",\"mcc3365\":\"Travel\",\"mcc3366\":\"Travel\",\"mcc3367\":\"Travel\",\"mcc3368\":\"Travel\",\"mcc3369\":\"Travel\",\"mcc3370\":\"Travel\",\"mcc3371\":\"Travel\","
"\"mcc3372\":\"Travel\",\"mcc3373\":\"Travel\",\"mcc3374\":\"Travel\",\"mcc3375\":\"Travel\",\"mcc3376\":\"Travel\",\"mcc3377\":\"Travel\",\"mcc3378\":\"Travel\",\"mcc3379\":\"Travel\",\"mcc3380\":\"Travel\","
"\"mcc3381\":\"Travel\",\"mcc3382\":\"Travel\",\"mcc3383\":\"Travel\",\"mcc3384\":\"Travel\",\"mcc3385\":\"Travel\",\"mcc3386\":\"Travel\",\"mcc3387\":\"Travel\",\"mcc3388\":\"Travel\",\"mcc3389\":\"Travel\","
"\"mcc3390\":\"Travel\",\"mcc3391\":\"Travel\",\"mcc3392\":\"Travel\",\"mcc3393\":\"Travel\",\"mcc3394\":\"Travel\",\"mcc3395\":\"Travel\",\"mcc3396\":\"Travel\",\"mcc3397\":\"Travel\",\"mcc3398\":\"Travel\","
"\"mcc3399\":\"Travel\",\"mcc3400\":\"Travel\",\"mcc3401\":\"Travel\",\"mcc3402\":\"Travel\",\"mcc3403\":\"Travel\",\"mcc3404\":\"Travel\",\"mcc3405\":\"Travel\",\"mcc3406\":\"Travel\",\"mcc3407\":\"Travel\","
"\"mcc3408\":\"Travel\",\"mcc3409\":\"Travel\",\"mcc3410\":\"Travel\",\"mcc3411\":\"Travel\",\"mcc3412\":\"Travel\",\"mcc3413\":\"Travel\",\"mcc3414\":\"Travel\",\"mcc3415\":\"Travel\",\"mcc3416\":\"Travel\","
"\"mcc3417\":\"Travel\",\"mcc3418\":\"Travel\",\"mcc3419\":\"Travel\",\"mcc3420\":\"Travel\",\"mcc3421\":\"Travel\",\"mcc3422\":\"Travel\",\"mcc3423\":\"Travel\",\"mcc3424\":\"Travel\",\"mcc3425\":\"Travel\","
"\"mcc3426\":\"Travel\",\"mcc3427\":\"Travel\",\"mcc3428\":\"Travel\",\"mcc3429\":\"Travel\",\"mcc3430\":\"Travel\",\"mcc3431\":\"Travel\",\"mcc3432\":\"Travel\",\"mcc3433\":\"Travel\",\"mcc3434\":\"Travel\","
"\"mcc3435\":\"Travel\",\"mcc3436\":\"Travel\",\"mcc3437\":\"Travel\",\"mcc3438\":\"Travel\",\"mcc3439\":\"Travel\",\"mcc3440\":\"Travel\",\"mcc3441\":\"Travel\",\"mcc3501\":\"Travel\",\"mcc3502\":\"Travel\","
"\"mcc3503\":\"Travel\",\"mcc3504\":\"Travel\",\"mcc3505\":\"Travel\",\"mcc3506\":\"Travel\",\"mcc3507\":\"Travel\",\"mcc3508\":\"Travel\",\"mcc3509\":\"Travel\",\"mcc3510\":\"Travel\",\"mcc3511\":\"Travel\","
"\"mcc3512\":\"Travel\",\"mcc3513\":\"Travel\",\"mcc3514\":\"Travel\",\"mcc3515\":\"Travel\",\"mcc3516\":\"Travel\",\"mcc3517\":\"Travel\",\"mcc3518\":\"Travel\",\"mcc3519\":\"Travel\",\"mcc3520\":\"Travel\","
"\"mcc3521\":\"Travel\",\"mcc3522\":\"Travel\",\"mcc3523\":\"Travel\",\"mcc3524\":\"Travel\",\"mcc3525\":\"Travel\",\"mcc3526\":\"Travel\",\"mcc3527\":\"Travel\",\"mcc3528\":\"Travel\",\"mcc3529\":\"Travel\","
"\"mcc3530\":\"Travel\",\"mcc3531\":\"Travel\",\"mcc3532\":\"Travel\",\"mcc3533\":\"Travel\",\"mcc3534\":\"Travel\",\"mcc3535\":\"Travel\",\"mcc3536\":\"Travel\",\"mcc3537\":\"Travel\",\"mcc3538\":\"Travel\","
"\"mcc3539\":\"Travel\",\"mcc3540\":\"Travel\",\"mcc3541\":\"Travel\",\"mcc3542\":\"Travel\",\"mcc3543\":\"Travel\",\"mcc3544\":\"Travel\",\"mcc3545\":\"Travel\",\"mcc3546\":\"Travel\",\"mcc3547\":\"Travel\","
"\"mcc3548\":\"Travel\",\"mcc3549\":\"Travel\",\"mcc3550\":\"Travel\",\"mcc3551\":\"Travel\",\"mcc3552\":\"Travel\",\"mcc3553\":\"Travel\",\"mcc3554\":\"Travel\",\"mcc3555\":\"Travel\",\"mcc3556\":\"Travel\","
"\"mcc3557\":\"Travel\",\"mcc3558\":\"Travel\",\"mcc3559\":\"Travel\",\"mcc3560\":\"Travel\",\"mcc3561\":\"Travel\",\"mcc3562\":\"Travel\",\"mcc3563\":\"Travel\",\"mcc3564\":\"Travel\",\"mcc3565\":\"Travel\","
"\"mcc3566\":\"Travel\",\"mcc3567\":\"Travel\",\"mcc3568\":\"Travel\",\"mcc3569\":\"Travel\",\"mcc3570\":\"Travel\",\"mcc3571\":\"Travel\",\"mcc3572\":\"Travel\",\"mcc3573\":\"Travel\",\"mcc3574\":\"Travel\","
"\"mcc3575\":\"Travel\",\"mcc3576\":\"Travel\",\"mcc3577\":\"Travel\",\"mcc3578\":\"Travel\",\"mcc3579\":\"Travel\",\"mcc3580\":\"Travel\",\"mcc3581\":\"Travel\",\"mcc3582\":\"Travel\",\"mcc3583\":\"Travel\","
"\"mcc3584\":\"Travel\",\"mcc3585\":\"Travel\",\"mcc3586\":\"Travel\",\"mcc3587\":\"Travel\",\"mcc3588\":\"Travel\",\"mcc3589\":\"Travel\",\"mcc3590\":\"Travel\",\"mcc3591\":\"Travel\",\"mcc3592\":\"Travel\","
"\"mcc3593\":\"Travel\",\"mcc3594\":\"Travel\",\"mcc3595\":\"Travel\",\"mcc3596\":\"Travel\",\"mcc3597\":\"Travel\",\"mcc3598\":\"Travel\",\"mcc3599\":\"Travel\",\"mcc3600\":\"Travel\",\"mcc3601\":\"Travel\","
"\"mcc3602\":\"Travel\",\"mcc3603\":\"Travel\",\"mcc3604\":\"Travel\",\"mcc3605\":\"Travel\",\"mcc3606\":\"Travel\",\"mcc3607\":\"Travel\",\"mcc3608\":\"Travel\",\"mcc3609\":\"Travel\",\"mcc3610\":\"Travel\","
"\"mcc3611\":\"Travel\",\"mcc3612\":\"Travel\",\"mcc3613\":\"Travel\",\"mcc3614\":\"Travel\",\"mcc3615\":\"Travel\",\"mcc3616\":\"Travel\",\"mcc3617\":\"Travel\",\"mcc3618\":\"Travel\",\"mcc3619\":\"Travel\","
"\"mcc3620\":\"Travel\",\"mcc3621\":\"Travel\",\"mcc3622\":\"Travel\",\"mcc3623\":\"Travel\",\"mcc3624\":\"Travel\",\"mcc3625\":\"Travel\",\"mcc3626\":\"Travel\",\"mcc3627\":\"Travel\",\"mcc3628\":\"Travel\","
"\"mcc3629\":\"Travel\",\"mcc3630\":\"Travel\",\"mcc3631\":\"Travel\",\"mcc3632\":\"Travel\",\"mcc3633\":\"Travel\",\"mcc3634\":\"Travel\",\"mcc3635\":\"Travel\",\"mcc3636\":\"Travel\",\"mcc3637\":\"Travel\","
"\"mcc3638\":\"Travel\",\"mcc3639\":\"Travel\",\"mcc3640\":\"Travel\",\"mcc3641\":\"Travel\",\"mcc3642\":\"Travel\",\"mcc3643\":\"Travel\",\"mcc3644\":\"Travel\",\"mcc3645\":\"Travel\",\"mcc3646\":\"Travel\","
"\"mcc3647\":\"Travel\",\"mcc3648\":\"Travel\",\"mcc3649\":\"Travel\",\"mcc3650\":\"Travel\",\"mcc3651\":\"Travel\",\"mcc3652\":\"Travel\",\"mcc3653\":\"Travel\",\"mcc3654\":\"Travel\",\"mcc3655\":\"Travel\","
"\"mcc3656\":\"Travel\",\"mcc3657\":\"Travel\",\"mcc3658\":\"Travel\",\"mcc3659\":\"Travel\",\"mcc3660\":\"Travel\",\"mcc3661\":\"Travel\",\"mcc3662\":\"Travel\",\"mcc3663\":\"Travel\",\"mcc3664\":\"Travel\","
"\"mcc3665\":\"Travel\",\"mcc3666\":\"Travel\",\"mcc3667\":\"Travel\",\"mcc3668\":\"Travel\",\"mcc3669\":\"Travel\",\"mcc3670\":\"Travel\",\"mcc3671\":\"Travel\",\"mcc3672\":\"Travel\",\"mcc3673\":\"Travel\","
"\"mcc3674\":\"Travel\",\"mcc3675\":\"Travel\",\"mcc3676\":\"Travel\",\"mcc3677\":\"Travel\",\"mcc3678\":\"Travel\",\"mcc3679\":\"Travel\",\"mcc3680\":\"Travel\",\"mcc3681\":\"Travel\",\"mcc3682\":\"Travel\","
"\"mcc3683\":\"Travel\",\"mcc3684\":\"Travel\",\"mcc3685\":\"Travel\",\"mcc3686\":\"Travel\",\"mcc3687\":\"Travel\",\"mcc3688\":\"Travel\",\"mcc3689\":\"Travel\",\"mcc3690\":\"Travel\",\"mcc3691\":\"Travel\","
"\"mcc3692\":\"Travel\",\"mcc3693\":\"Travel\",\"mcc3694\":\"Travel\",\"mcc3695\":\"Travel\",\"mcc3696\":\"Travel\",\"mcc3697\":\"Travel\",\"mcc3698\":\"Travel\",\"mcc3699\":\"Travel\",\"mcc3700\":\"Travel\","
"\"mcc3701\":\"Travel\",\"mcc3702\":\"Travel\",\"mcc3703\":\"Travel\",\"mcc3704\":\"Travel\",\"mcc3705\":\"Travel\",\"mcc3706\":\"Travel\",\"mcc3707\":\"Travel\",\"mcc3708\":\"Travel\",\"mcc3709\":\"Travel\","
"\"mcc3710\":\"Travel\",\"mcc3711\":\"Travel\",\"mcc3712\":\"Travel\",\"mcc3713\":\"Travel\",\"mcc3714\":\"Travel\",\"mcc3715\":\"Travel\",\"mcc3716\":\"Travel\",\"mcc3717\":\"Travel\",\"mcc3718\":\"Travel\","
"\"mcc3719\":\"Travel\",\"mcc3720\":\"Travel\",\"mcc3721\":\"Travel\",\"mcc3722\":\"Travel\",\"mcc3723\":\"Travel\",\"mcc3724\":\"Travel\",\"mcc3725\":\"Travel\",\"mcc3726\":\"Travel\",\"mcc3727\":\"Travel\","
"\"mcc3728\":\"Travel\",\"mcc3729\":\"Travel\",\"mcc3730\":\"Travel\",\"mcc3731\":\"Travel\",\"mcc3732\":\"Travel\",\"mcc3733\":\"Travel\",\"mcc3734\":\"Travel\",\"mcc3735\":\"Travel\",\"mcc3736\":\"Travel\","
"\"mcc3737\":\"Travel\",\"mcc3738\":\"Travel\",\"mcc3739\":\"Travel\",\"mcc3740\":\"Travel\",\"mcc3741\":\"Travel\",\"mcc3742\":\"Travel\",\"mcc3743\":\"Travel\",\"mcc3744\":\"Travel\",\"mcc3745\":\"Travel\","
"\"mcc3746\":\"Travel\",\"mcc3747\":\"Travel\",\"mcc3748\":\"Travel\",\"mcc3749\":\"Travel\",\"mcc3750\":\"Travel\",\"mcc3751\":\"Travel\",\"mcc3752\":\"Travel\",\"mcc3753\":\"Travel\",\"mcc3754\":\"Travel\","
"\"mcc3755\":\"Travel\",\"mcc3756\":\"Travel\",\"mcc3757\":\"Travel\",\"mcc3758\":\"Travel\",\"mcc3759\":\"Travel\",\"mcc3760\":\"Travel\",\"mcc3761\":\"Travel\",\"mcc3762\":\"Travel\",\"mcc3763\":\"Travel\","
"\"mcc3764\":\"Travel\",\"mcc3765\":\"Travel\",\"mcc3766\":\"Travel\",\"mcc3767\":\"Travel\",\"mcc3768\":\"Travel\",\"mcc3769\":\"Travel\",\"mcc3770\":\"Travel\",\"mcc3771\":\"Travel\",\"mcc3772\":\"Travel\","
"\"mcc3773\":\"Travel\",\"mcc3774\":\"Travel\",\"mcc3775\":\"Travel\",\"mcc3776\":\"Travel\",\"mcc3777\":\"Travel\",\"mcc3778\":\"Travel\",\"mcc3779\":\"Travel\",\"mcc3780\":\"Travel\",\"mcc3781\":\"Travel\","
"\"mcc3782\":\"Travel\",\"mcc3783\":\"Travel\",\"mcc3784\":\"Travel\",\"mcc3785\":\"Travel\",\"mcc3786\":\"Travel\",\"mcc3787\":\"Travel\",\"mcc3788\":\"Travel\",\"mcc3789\":\"Travel\",\"mcc3790\":\"Travel\","
"\"mcc3825\":\"Travel\",\"mcc3826\":\"Travel\",\"mcc3831\":\"Travel\",\"mcc3832\":\"Travel\",\"mcc3833\":\"Travel\",\"mcc3835\":\"Travel\",\"mcc3838\":\"Travel\",\"mcc4011\":\"Car\",\"mcc4111\":\"Car\","
"\"mcc4112\":\"Car\",\"mcc4119\":\"Professional Services\",\"mcc4121\":\"Travel\",\"mcc4131\":\"Car\",\"mcc4214\":\"Office Supplies\",\"mcc4215\":\"Office Supplies\",\"mcc4225\":\"Other\","
"\"mcc4411\":\"Professional Services\",\"mcc4457\":\"Travel\",\"mcc4468\":\"Materials\",\"mcc4511\":\"Travel\",\"mcc4582\":\"Travel\",\"mcc4722\":\"Travel\",\"mcc4723\":\"Travel\",\"mcc4784\":\"Car\","
"\"mcc4789\":\"Car\",\"mcc4812\":\"Utilities\",\"mcc4814\":\"Utilities\",\"mcc4815\":\"Utilities\",\"mcc4816\":\"Utilities\",\"mcc4821\":\"Utilities\",\"mcc4829\":\"Other\",\"mcc4899\":\"Utilities\","
"\"mcc4900\":\"Utilities\",\"mcc5013\":\"Materials\",\"mcc5021\":\"Materials\",\"mcc5039\":\"Materials\",\"mcc5044\":\"Materials\",\"mcc5045\":\"Materials\",\"mcc5046\":\"Materials\",\"mcc5047\":\"Materials\","
"\"mcc5051\":\"Materials\",\"mcc5065\":\"Materials\",\"mcc5072\":\"Materials\",\"mcc5074\":\"Materials\",\"mcc5085\":\"Materials\",\"mcc5094\":\"Materials\",\"mcc5099\":\"Materials\",\"mcc5111\":\"Materials\","
"\"mcc5122\":\"Materials\",\"mcc5131\":\"Materials\",\"mcc5137\":\"Materials\",\"mcc5139\":\"Materials\",\"mcc5169\":\"Materials\",\"mcc5172\":\"Materials\",\"mcc5192\":\"Materials\",\"mcc5193\":\"Materials\","
"\"mcc5198\":\"Materials\",\"mcc5199\":\"Materials\",\"mcc5200\":\"Materials\",\"mcc5211\":\"Materials\",\"mcc5231\":\"Materials\",\"mcc5251\":\"Materials\",\"mcc5261\":\"Materials\",\"mcc5271\":\"Materials\","
"\"mcc5300\":\"Materials\",\"mcc5309\":\"Materials\",\"mcc5310\":\"Materials\",\"mcc5311\":\"Materials\",\"mcc5331\":\"Materials\",\"mcc5399\":\"Materials\",\"mcc5411\":\"Meals and Entertainment\","
"\"mcc5422\":\"Meals and Entertainment\",\"mcc5441\":\"Meals and Entertainment\",\"mcc5451\":\"Meals and Entertainment\",\"mcc5462\":\"Meals and Entertainment\",\"mcc5499\":\"Meals and Entertainment\","
"\"mcc5511\":\"Materials\",\"mcc5521\":\"Materials\",\"mcc5531\":"
"\"Materials\",\"mcc5532\":\"Materials\",\"mcc5533\":\"Materials\",\"mcc5541\":\"Car\",\"mcc5542\":\"Car\",\"mcc5551\":\"Materials\",\"mcc5561\":\"Materials\",\"mcc5571\":\"Materials\",\"mcc5592\":\"Materials\","
"\"mcc5598\":\"Materials\",\"mcc5599\":\"Materials\",\"mcc5611\":\"Materials\",\"mcc5621\":\"Materials\",\"mcc5631\":\"Materials\",\"mcc5641\":\"Materials\",\"mcc5651\":\"Materials\",\"mcc5655\":\"Materials\","
"\"mcc5661\":\"Materials\",\"mcc5681\":\"Materials\",\"mcc5691\":\"Materials\",\"mcc5697\":\"Materials\",\"mcc5698\":\"Materials\",\"mcc5699\":\"Materials\",\"mcc5712\":\"Materials\",\"mcc5713\":\"Materials\","
"\"mcc5714\":\"Materials\",\"mcc5718\":\"Materials\",\"mcc5719\":\"Materials\",\"mcc5722\":\"Materials\",\"mcc5732\":\"Materials\",\"mcc5733\":\"Materials\",\"mcc5734\":\"Materials\",\"mcc5735\":\"Materials\","
"\"mcc5811\":\"Meals and Entertainment\",\"mcc5812\":\"Meals and Entertainment\",\"mcc5813\":\"Meals and Entertainment\",\"mcc5814\":\"Meals and Entertainment\",\"mcc5815\":\"Materials\",\"mcc5816\":\"Materials\","
""
"\"mcc5817\":\"Materials\",\"mcc5818\":\"Materials\",\"mcc5832\":\"Other\",\"mcc5912\":\"Meals and Entertainment\",\"mcc5921\":\"Meals and Entertainment\",\"mcc5931\":\"Materials\",\"mcc5932\":\"Materials\","
"\"mcc5933\":\"Materials\",\"mcc5935\":\"Materials\",\"mcc5937\":\"Materials\",\"mcc5940\":\"Materials\",\"mcc5941\":\"Materials\",\"mcc5942\":\"Materials\",\"mcc5943\":\"Materials\",\"mcc5944\":\"Materials\","
"\"mcc5945\":\"Materials\",\"mcc5946\":\"Materials\",\"mcc5947\":\"Materials\",\"mcc5948\":\"Materials\",\"mcc5949\":\"Materials\",\"mcc5950\":\"Materials\",\"mcc5960\":\"Professional Services\",\"mcc5961\":\"Other\","
"\"mcc5962\":\"Travel\",\"mcc5963\":\"Materials\",\"mcc5964\":\"Materials\",\"mcc5965\":\"Materials\",\"mcc5966\":\"Materials\",\"mcc5967\":\"Materials\",\"mcc5968\":\"Professional Services\",\"mcc5969\":\"Other\","
"\"mcc5970\":\"Materials\",\"mcc5971\":\"Materials\",\"mcc5972\":\"Materials\",\"mcc5973\":\"Materials\",\"mcc5975\":\"Materials\",\"mcc5976\":\"Materials\",\"mcc5977\":\"Materials\",\"mcc5978\":\"Materials\","
"\"mcc5983\":\"Materials\",\"mcc5992\":\"Materials\",\"mcc5993\":\"Materials\",\"mcc5994\":\"Materials\",\"mcc5995\":\"Materials\",\"mcc5996\":\"Materials\",\"mcc5997\":\"Materials\",\"mcc5998\":\"Materials\","
"\"mcc5999\":\"Materials\",\"mcc6010\":\"Other\",\"mcc6011\":\"Other\",\"mcc6012\":\"Other\",\"mcc6051\":\"Other\",\"mcc6211\":\"Other\",\"mcc6300\":\"Professional Services\",\"mcc6381\":\"Other\","
"\"mcc6399\":\"Professional Services\",\"mcc6513\":\"Other\",\"mcc6530\":\"Other\",\"mcc6531\":\"Other\",\"mcc6532\":\"Other\",\"mcc6533\":\"Other\",\"mcc6534\":\"Other\",\"mcc6535\":\"Other\",\"mcc6540\":"
"\"Other\",\"mcc6611\":\"Other\",\"mcc6760\":\"Other\",\"mcc7011\":\"Travel\",\"mcc7012\":\"Travel\",\"mcc7032\":\"Professional Services\",\"mcc7033\":\"Professional Services\","
"\"mcc7210\":\"Professional Services\",\"mcc7211\":\"Professional Services\",\"mcc7216\":\"Professional Services\",\"mcc7217\":\"Professional Services\",\"mcc7221\":\"Professional Services\","
"\"mcc7230\":\"Professional Services\",\"mcc7251\":\"Professional Services\",\"mcc7261\":\"Professional Services\",\"mcc7273\":\"Professional Services\",\"mcc7276\":\"Professional Services\","
"\"mcc7277\":\"Professional Services\",\"mcc7278\":\"Professional Services\",\"mcc7296\":\"Professional Services\",\"mcc7297\":\"Professional Services\",\"mcc7298\":\"Professional Services\","
"\"mcc7299\":\"Professional Services\",\"mcc7311\":\"Professional Services\",\"mcc7321\":\"Professional Services\",\"mcc7332\":\"Other\",\"mcc7333\":\"Professional Services\",\"mcc7338\":\"Professional Services\","
"\"mcc7339\":\"Professional Services\",\"mcc7342\":\"Professional Services\",\"mcc7349\":\"Professional Services\",\"mcc7361\":\"Professional Services\",\"mcc7372\":\"Professional Services\","
"\"mcc7375\":\"Professional Services\",\"mcc7379\":\"Professional Services\",\"mcc7392\":\"Professional Services\",\"mcc7393\":\"Professional Services\",\"mcc7394\":\"Professional Services\","
"\"mcc7395\":\"Professional Services\",\"mcc7399\":\"Professional Services\",\"mcc742\":\"Professional Services\",\"mcc7511\":\"Car\",\"mcc7512\":\"Travel\",\"mcc7513\":\"Travel\","
"\"mcc7519\":\"Travel\",\"mcc7523\":\"Car\",\"mcc7531\":\"Materials\",\"mcc7534\":\"Materials\",\"mcc7535\":\"Materials\",\"mcc7538\":\"Materials\",\"mcc7542\":\"Materials\","
"\"mcc7549\":\"Materials\",\"mcc7622\":\"Professional Services\",\"mcc7623\":\"Professional Services\",\"mcc7629\":\"Professional Services\",\"mcc763\":\"Other\",\"mcc7631\":\"Professional Services\","
"\"mcc7641\":\"Professional Services\",\"mcc7692\":\"Professional Services\",\"mcc7699\":\"Professional Services\",\"mcc780\":\"Professional Services\",\"mcc7829\":\"Professional Services\",\"mcc7832\":"
"\"Professional Services\",\"mcc7841\":\"Professional Services\",\"mcc7911\":\"Professional Services\",\"mcc7922\":\"Professional Services\",\"mcc7929\":\"Professional Services\","
"\"mcc7932\":\"Professional Services\",\"mcc7933\":\"Professional Services\",\"mcc7941\":\"Professional Services\",\"mcc7991\":\"Professional Services\",\"mcc7992\":\"Professional Services\","
"\"mcc7993\":\"Professional Services\",\"mcc7994\":\"Professional Services\",\"mcc7995\":\"Professional Services\",\"mcc7996\":\"Professional Services\",\"mcc7997\":\"Professional Services\","
"\"mcc7998\":\"Professional Services\",\"mcc7999\":\"Professional Services\",\"mcc8011\":\"Professional Services\",\"mcc8021\":\"Professional Services\",\"mcc8031\":\"Professional Services\","
"\"mcc8041\":\"Professional Services\",\"mcc8042\":\"Professional Services\",\"mcc8043\":\"Professional Services\",\"mcc8044\":\"Other\",\"mcc8049\":\"Professional Services\","
"\"mcc8050\":\"Professional Services\",\"mcc8062\":\"Professional Services\",\"mcc8071\":\"Professional Services\",\"mcc8099\":\"Professional Services\",\"mcc8111\":\"Professional Services\","
"\"mcc8211\":\"Professional Services\",\"mcc8220\":\"Professional Services\",\"mcc8241\":\"Professional Services\",\"mcc8244\":\"Professional Services\",\"mcc8249\":\"Professional Services\","
"\"mcc8299\":\"Professional Services\",\"mcc8351\":\"Professional Services\",\"mcc8398\":\"Other\",\"mcc8641\":\"Other\",\"mcc8651\":\"Other\",\"mcc8661\":"
"\"Other\",\"mcc8675\":\"Materials\",\"mcc8699\":\"Other\",\"mcc8734\":\"Professional Services\",\"mcc8911\":\"Professional Services\",\"mcc8931\":\"Professional Services\","
"\"mcc8999\":\"Professional Services\",\"mcc9211\":\"Professional Services\",\"mcc9222\":\"Professional Services\",\"mcc9223\":\"Professional Services\",\"mcc9311\":\"Professional Services\","
"\"mcc9399\":\"Professional Services\",\"mcc9402\":\"Office Supplies\",\"mcc9405\":\"Professional Services\",\"mcc9700\":\"Other\",\"mcc9701\":\"Other\",\"mcc9702\":\"Professional Services\","
"\"mcc9950\":\"Other\"},\"modifiedCategories\":false,\"modifiedTags\":false,\"name\":\"New Policy\",\"outputCurrency\":\"USD\",\"owner\":\"test_user_userx1699552996668.6@testuser.testify.com\","
"\"requiresCategory\":false,\"requiresTag\":false,\"rter\":{\"visibility\":{\"enabled\":true}},"
"\"strictWorkflow\":true,\"tagLists\":[{\"tagListData\":{\"name\":\"Tag\",\"tags\":[]}}],\"tagObjects\":[],\"tax\":{\"trackingEnabled\":false},"
"\"technicalContact\":\"test_user_userx1699552996668.6@testuser.testify.com\",\"type\":\"corporate\",\"units\":{\"time\":{\"enabled\":false,\"rate\":50}},"
"\"welcomeNote\":[],\"chatReportIDAnnounce\":6177740978842143,\"chatReportIDAdmins\":5767600567542886,\"connections\":{\"quickbooksOnline\":{\"config\":{\"realmId\":\"12345\","
"\"autoSync\":{\"enabled\":false,\"jobID\":null}}},\"netsuite\":{\"accountID\":\"1234XYZ\",\"config\":{\"irrelevant\":\"12345\",\"autoSync\":{\"enabled\":true,\"jobID\":\"8867546407680421876\"}}}},"
"\"joinRequests\":[],\"policyExpenseChatIDs\":null}";

const string BedrockPlugin_DB::name("DB");
const string& BedrockPlugin_DB::getName() const {
    return name;
}

BedrockPlugin_DB::BedrockPlugin_DB(BedrockServer& s) : BedrockPlugin(s)
{
}

void BedrockPlugin_DB::upgradeDatabase(SQLite& db) {
    bool created = false;
    SASSERT(db.verifyTable("nameValuePairs",
                           "CREATE TABLE nameValuePairs ( "
                           "accountID INTEGER NOT NULL, "
                           "name      TEXT    NOT NULL, "
                           "value     TEXT    NOT NULL, "
                           "created   DATE    NOT NULL DEFAULT '2008-01-01' ) ",
                           created
    ));
    SASSERT(db.verifyIndex("nameValuePairsAccountIDName", "nameValuePairs", "( accountID, name )", true, true));
    SASSERT(db.verifyIndex("nameValuePairsName", "nameValuePairs", "( name )", false, true));
    SASSERT(db.verifyIndex("nameValuePairsUnwonGuidesDealsPolicy", "nameValuePairs", "(name, JSON_EXTRACT(value, '$.assignedGuide.email'), accountID, value) WHERE name GLOB 'expensify_policy*' AND JSON_VALID(value) AND JSON_EXTRACT(value, '$.type') IN ('free', 'team', 'corporate') AND (JSON_EXTRACT(value, '$.assignedGuide.won') = 'false' OR JSON_EXTRACT(value, '$.assignedGuide.wonTimestamp') = '')", false, true));
    SASSERT(db.verifyIndex("nameValuePairsUnwonGuidesDealsDomain", "nameValuePairs", "(name, JSON_EXTRACT(value, '$.email'), accountID, value) WHERE name='assignedGuide' AND JSON_VALID(value) AND (JSON_EXTRACT(value, '$.won') = 'false' OR JSON_EXTRACT(value, '$.wonTimestamp') = '')", false, true));
    SASSERT(db.verifyIndex("nameValuePairsSubscriptionType", "nameValuePairs", "(name, JSON_EXTRACT(value, '$.type'), accountID, value) WHERE name = 'private_subscription' AND JSON_VALID(value)", false, true));
    SASSERT(db.verifyIndex("nameValuePairsFreePolicies", "nameValuePairs", "(name, accountID) WHERE name GLOB 'expensify_policy*' AND JSON_VALID(value) AND JSON_EXTRACT(value, '$.type') = 'free'", false, true));

    db.write("INSERT OR REPLACE INTO nameValuePairs VALUES(1, 'test_policy', " + SQ(test_policy) +", '2023-11-09');");
}

BedrockDBCommand::BedrockDBCommand(SQLiteCommand&& baseCommand, BedrockPlugin_DB* plugin) :
  BedrockCommand(move(baseCommand), plugin),
    // The "full" syntax of a query request is:
    //
    //      Query
    //      Query: ...sql...
    //
    // However, that's awfully redundant.  As shorthand, we'll accept the query
    // in the method line as follows:
    //
    //      Query: ...sql...
  query(STrim(SStartsWith(SToLower(request.methodLine), "query:") ? request.methodLine.substr(strlen("query:")) : request["query"]))
{
}

unique_ptr<BedrockCommand> BedrockPlugin_DB::getCommand(SQLiteCommand&& baseCommand) {
    if (SStartsWith(SToLower(baseCommand.request.methodLine), "query:") || SIEquals(baseCommand.request.getVerb(), "Query")) {
        return make_unique<BedrockDBCommand>(move(baseCommand), this);
    }
    return nullptr;
}

bool BedrockDBCommand::peek(SQLite& db) {
    if (query.size() < 1 || query.size() > BedrockPlugin::MAX_SIZE_QUERY) {
        STHROW("402 Missing query");
    }

    if (!SEndsWith(query, ";")) {
        SALERT("Query aborted, query must end in ';'");
        STHROW("502 Query Missing Semicolon");
    }

    // Get a list of prepared statements from the database.
    list<sqlite3_stmt*> statements;
    int prepareResult = db.getPreparedStatements(query, statements);

    // Check each one to see if it's a write, and then release it.
    bool write = false;
    for (sqlite3_stmt* statement : statements) {
        if (!sqlite3_stmt_readonly(statement)) {
            write = true;
        }
        sqlite3_finalize(statement);
    }

    // If we got any errors while preparing, we're calling this a bad command.
    if (prepareResult != SQLITE_OK) {
        STHROW("402 Bad query");
    }

    // If anything was a write, escalate to `process`.
    if (write) {
        return false;
    }

    // Attempt the read-only query
    SQResult result;
    if (!db.read(query, result)) {
        STHROW("402 Bad query");
    }

    // Worked! Set the output and return.
    response.content = result.serialize(request["Format"]);

    return true;
}

void BedrockDBCommand::process(SQLite& db) {
    if (db.getUpdateNoopMode()) {
        SINFO("Query run in mocked request, just ignoring.");
        return;
    }
    BedrockPlugin::verifyAttributeBool(request, "nowhere",  false);

    const string upperQuery = SToUpper(query);
    if (!request.test("nowhere") &&
        (SStartsWith(upperQuery, "UPDATE") || SStartsWith(upperQuery, "DELETE")) &&
        !SContains(upperQuery, " WHERE ")) {
        SALERT("Query aborted, it has no 'where' clause: '" << query << "'");
        STHROW("502 Query aborted");
    }

    // Attempt the query
    if (!db.write(query)) {
        // Query failed
        SALERT("Query failed: '" << query << "'");
        response["error"] = db.getLastError();
        STHROW("502 Query failed");
    }

    // Worked! Let's save the last inserted row id
    if (SStartsWith(upperQuery, "INSERT ")) {
        response["lastInsertRowID"] = SToStr(db.getLastInsertRowID());
    }

    // Successfully processed
    return;
}
