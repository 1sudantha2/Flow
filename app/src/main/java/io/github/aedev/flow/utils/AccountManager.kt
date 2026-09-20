package io.github.aedev.flow.utils

import android.content.Context
import android.util.Log
import io.github.aedev.flow.innertube.YouTube
import io.github.aedev.flow.innertube.utils.parseCookieString
import io.github.aedev.flow.innertube.utils.sha1
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * The signed-in Google account session.
 *
 * [cookie] is the raw cookie header value for `.youtube.com`, exactly as a signed-in browser would
 * send it. [dataSyncId] is the InnerTube account identifier (SHA-1 of the account email and
 * SAPISID), passed as `context.user.onBehalfOfUser` so InnerTube routes requests to the account's
 * own data.
 */
data class AccountSession(
    val cookie: String,
    val email: String?,
    val name: String?,
    val thumbnailUrl: String?,
    val dataSyncId: String?,
)

/**
 * Owner of the signed-in YouTube session.
 *
 * Sign-in happens in [io.github.aedev.flow.ui.screens.account.LoginScreen]: the accounts.google.com
 * page runs in a WebView and its `.youtube.com` cookies are captured once YouTube redirects back.
 * Everything InnerTube needs for an authenticated request — the cookie header, the SAPISIDHASH
 * Authorization header and the dataSyncId — is derived from those cookies and applied to
 * [YouTube] here, so every browse/reel/next/player call carries the account automatically.
 *
 * Storage is plain app-private SharedPreferences: the cookie never leaves the device except to
 * YouTube itself, same as any other client that keeps a session.
 */
object AccountManager {
    private const val TAG = "AccountManager"
    private const val PREFS_NAME = "flow_account"
    private const val KEY_COOKIE = "cookie"
    private const val KEY_EMAIL = "email"
    private const val KEY_NAME = "name"
    private const val KEY_THUMBNAIL_URL = "thumbnail_url"
    private const val KEY_DATA_SYNC_ID = "data_sync_id"

    private val _session = MutableStateFlow<AccountSession?>(null)
    val session: StateFlow<AccountSession?> = _session.asStateFlow()

    val isLoggedIn: Boolean
        get() = _session.value != null

    /** The minimum cookies an authenticated `.youtube.com` session must carry. */
    fun isUsableCookie(cookie: String): Boolean {
        val map = parseCookieString(cookie)
        return map.containsKey("SID") &&
            (map.containsKey("SAPISID") || map.containsKey("__Secure-3PAPISID") || map.containsKey("LOGIN_INFO"))
    }

    /**
     * Restore the saved session at process start, before any feed loads. Returns true when a
     * session was applied.
     */
    fun restore(context: Context): Boolean {
        val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        val cookie = prefs.getString(KEY_COOKIE, null)?.takeIf(String::isNotBlank)
        if (cookie == null || !isUsableCookie(cookie)) {
            if (cookie != null) logout(context)
            return false
        }
        apply(
            AccountSession(
                cookie = cookie,
                email = prefs.getString(KEY_EMAIL, null),
                name = prefs.getString(KEY_NAME, null),
                thumbnailUrl = prefs.getString(KEY_THUMBNAIL_URL, null),
                dataSyncId = prefs.getString(KEY_DATA_SYNC_ID, null),
            ),
        )
        Log.d(TAG, "account session restored")
        return true
    }

    /**
     * Complete a sign-in with the `.youtube.com` cookie string captured from the login WebView.
     *
     * The account's email/name are resolved through InnerTube's `account_menu` endpoint; that call
     * is best-effort — the cookie is authoritative, so a failure only costs the profile display
     * (and the dataSyncId, which needs the email), not the session itself.
     */
    suspend fun login(
        context: Context,
        cookie: String,
    ): Result<AccountSession> = runCatching {
        if (!isUsableCookie(cookie)) {
            throw IllegalStateException("Cookie is missing the YouTube session cookies")
        }
        val cookieMap = parseCookieString(cookie)
        val accountInfo = YouTube.accountInfo().getOrNull()
        val email = accountInfo?.email
        val sapisid = cookieMap["SAPISID"]
        val dataSyncId = if (email != null && sapisid != null) getDataSyncId(email, sapisid) else null
        val session =
            AccountSession(
                cookie = cookie,
                email = email,
                name = accountInfo?.name,
                thumbnailUrl = accountInfo?.thumbnailUrl,
                dataSyncId = dataSyncId,
            )
        apply(session)
        context
            .getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .edit()
            .putString(KEY_COOKIE, session.cookie)
            .putString(KEY_EMAIL, session.email)
            .putString(KEY_NAME, session.name)
            .putString(KEY_THUMBNAIL_URL, session.thumbnailUrl)
            .putString(KEY_DATA_SYNC_ID, session.dataSyncId)
            .apply()
        Log.d(TAG, "signed in as ${session.email ?: "(email unavailable)"}")
        session
    }

    fun logout(context: Context) {
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE).edit().clear().apply()
        YouTube.cookie = null
        YouTube.dataSyncId = null
        YouTube.useLoginForBrowse = false
        _session.value = null
        Log.d(TAG, "signed out")
    }

    private fun apply(session: AccountSession) {
        YouTube.cookie = session.cookie
        YouTube.dataSyncId = session.dataSyncId
        YouTube.useLoginForBrowse = true
        _session.value = session
    }
}

/** The InnerTube dataSyncId: SHA-1 over "<email> <SAPISID>", hex-encoded. */
private fun getDataSyncId(
    accountEmail: String,
    sapisid: String,
): String = sha1("$accountEmail $sapisid")
