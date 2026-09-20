package io.github.aedev.flow.ui.screens.account

import android.annotation.SuppressLint
import android.net.Uri
import android.util.Log
import android.webkit.CookieManager
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Toast
import androidx.activity.compose.BackHandler
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.WindowInsets
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import io.github.aedev.flow.FlowApplication
import io.github.aedev.flow.R
import io.github.aedev.flow.ui.components.layout.topbar.FlowTopBar
import io.github.aedev.flow.utils.AccountManager
import kotlinx.coroutines.launch

private const val TAG = "LoginScreen"

/**
 * The Google sign-in page, in a WebView.
 *
 * The flow is the one youtube.com itself uses: sign in at accounts.google.com, continue back to
 * YouTube. Once the redirect lands on a youtube.com host, the `.youtube.com` session cookies
 * (SID, SAPISID, __Secure-*PSID*…) are read from the WebView cookie jar and handed to
 * [AccountManager.login]; from that point every InnerTube request is made as the account.
 *
 * A desktop Chrome user agent is required — Google refuses embedded WebView user agents on the
 * sign-in form.
 */
@Composable
fun LoginScreen(
    onNavigateBack: () -> Unit,
    onLoginComplete: () -> Unit,
) {
    val scope = rememberCoroutineScope()
    val session by AccountManager.session.collectAsStateWithLifecycle()
    var completing by remember { mutableStateOf(false) }
    val signInError = stringResource(R.string.account_sign_in_error)

    BackHandler(enabled = completing) { /* swallow back while the session is being finalised */ }

    Scaffold(
        contentWindowInsets = WindowInsets(0.dp),
        topBar = {
            FlowTopBar(
                title = stringResource(R.string.account_sign_in),
                onBack = onNavigateBack,
            )
        },
        containerColor = MaterialTheme.colorScheme.background,
    ) { innerPadding ->
        Box(
            modifier =
                Modifier
                    .fillMaxSize()
                    .padding(innerPadding),
        ) {
            LoginWebView(
                modifier = Modifier.fillMaxSize(),
                onLoginCookies = { cookie ->
                    if (!completing) {
                        completing = true
                        scope.launch {
                            val result = AccountManager.login(FlowApplication.appContext, cookie)
                            if (result.isSuccess) {
                                onLoginComplete()
                            } else {
                                completing = false
                                Log.w(TAG, "sign-in failed: ${result.exceptionOrNull()?.message}")
                                Toast.makeText(FlowApplication.appContext, signInError, Toast.LENGTH_LONG).show()
                            }
                        }
                    }
                },
            )
            if (completing || session != null) {
                CircularProgressIndicator(modifier = Modifier.align(Alignment.Center))
            }
        }
    }
}

@SuppressLint("SetJavaScriptEnabled")
@Composable
private fun LoginWebView(
    modifier: Modifier = Modifier,
    onLoginCookies: (String) -> Unit,
) {
    AndroidView(
        modifier = modifier,
        factory = { context ->
            WebView(context).apply {
                settings.javaScriptEnabled = true
                settings.domStorageEnabled = true
                settings.userAgentString = CHROME_DESKTOP_USER_AGENT
                val cookieManager = CookieManager.getInstance()
                cookieManager.setAcceptCookie(true)
                cookieManager.setAcceptThirdPartyCookies(this, true)
                webViewClient =
                    object : WebViewClient() {
                        override fun shouldOverrideUrlLoading(
                            view: WebView,
                            request: WebResourceRequest,
                        ): Boolean = false

                        override fun doUpdateVisitedHistory(
                            view: WebView,
                            url: String?,
                            isReload: Boolean,
                        ) {
                            val cookie = extractSessionCookie(url) ?: return
                            onLoginCookies(cookie)
                        }
                    }
                loadUrl(SIGN_IN_URL)
            }
        },
    )
}

/**
 * Reads the `.youtube.com` cookies once [url] has landed on a YouTube host, returning the raw
 * cookie header when it carries a full session.
 */
private fun extractSessionCookie(url: String?): String? {
    val host = Uri.parse(url ?: return null).host ?: return null
    val onYouTube = host == "youtube.com" || host.endsWith(".youtube.com")
    if (!onYouTube) return null
    val cookie = CookieManager.getInstance().getCookie("https://www.youtube.com") ?: return null
    return cookie.takeIf { AccountManager.isUsableCookie(it) }
}

private const val SIGN_IN_URL =
    "https://accounts.google.com/ServiceLogin?ltmpl=sso&passive=true&service=youtube" +
        "&continue=https%3A%2F%2Fwww.youtube.com%2F&uilel=3&hl=en"

private const val CHROME_DESKTOP_USER_AGENT =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
