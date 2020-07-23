using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure;
using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace AzureSimplifiedStd.KeyVault
{
    public class KeyVault
    {
        private KeyVaultClient vault;
        private string clientAppID;
        private string clientAppSecret;
        private string targetTenantID;
        private string vaulturi;
        X509Certificate2 authCertificate;
        public KeyVault(string appID, string tenantID, string appSecret, string vaultURI = "")
        {
            clientAppID = appID;
            targetTenantID = tenantID;
            clientAppSecret = appSecret;
            vaulturi = vaultURI;
            AuthenticateBySecret();
        }
        public KeyVault(string appID, string tenantID, X509Certificate2 certificate, string vaultURI = "")
        {
            clientAppID = appID;
            targetTenantID = tenantID;
            authCertificate = certificate;
            vaulturi = vaultURI;
            AuthenticateByCert();
        }



        public void AuthenticateByCert()
        {
            vault = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(GetTokenFromCertAsync));
        }
        public void AuthenticateBySecret()
        {
            vault = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(GetTokenFromSecretAsync));
        }
        //Secrets
        public void CreateSecret(string secretName, byte[] secretValue, string vaultURI = "", bool secretIsEnabled = true, DateTime? expirationDate = null, DateTime? notEnabledBeforeThisDate = null, Dictionary<string, string> tags = null)
        {
            string secretbase64 = Convert.ToBase64String(secretValue);
            string uri = string.Empty;
            if (!string.IsNullOrEmpty(vaultURI))
            {
                uri = vaultURI;
            }
            else
            {
                uri = vaulturi;
            }
            SecretAttributes attr = new SecretAttributes();
            attr.Expires = expirationDate;
            attr.NotBefore = notEnabledBeforeThisDate;
            attr.Enabled = secretIsEnabled;

            if (string.IsNullOrEmpty(uri))
            {
                vault.SetSecretAsync(uri, secretName, secretbase64, tags, "base64String", attr);
            }
            else
            {
                throw new KeyVaultErrorException("Vault URI not set. Please use SetVault().");
            }
        }
        public void CreateSecret(string secretName, string secretValue, string vaultURI = "", bool secretIsEnabled = true, DateTime? expirationDate = null, DateTime? notEnabledBeforeThisDate = null, Dictionary<string, string> tags = null)
        {
            SecretAttributes attr = new SecretAttributes();
            attr.Expires = expirationDate;
            attr.NotBefore = notEnabledBeforeThisDate;
            attr.Enabled = secretIsEnabled;
            string uri = string.Empty;
            if (!string.IsNullOrEmpty(vaultURI))
            {
                uri = vaultURI;
            }
            else
            {
                uri = vaulturi;
            }

            if (string.IsNullOrEmpty(uri))
            {
                vault.SetSecretAsync(uri, secretName, secretValue, tags, "string", attr);
            }
            else
            {
                throw new KeyVaultErrorException("Vault URI not set. Please use SetVault().");
            }
        }
        public string GetSecret(string secretName)
        {
            return GetSecretAsync(secretName).Result;
        }
        public string GetSecretFromURI(string secretURI)
        {
            return GetSecretAsync(secretURI).Result;
        }
        public List<string> GetSecretsVersion(string secretName, string vaultURI = "")
        {
            string uri = string.Empty;
            if (!string.IsNullOrEmpty(vaultURI))
            {
                uri = vaultURI;
            }
            else
            {
                uri = vaulturi;
            }

            if (string.IsNullOrEmpty(uri))
            {
                return GetSecretsVersionAsync(uri, secretName).Result;
            }
            else
            {
                throw new KeyVaultErrorException("Vault URI not set. Please use SetVault().");
            }

        }
        public List<string> GetSecrets(string vaultURI = "")
        {
            string uri = string.Empty;
            if (!string.IsNullOrEmpty(vaultURI))
            {
                uri = vaultURI;
            }
            else
            {
                uri = vaulturi;
            }

            if (string.IsNullOrEmpty(uri))
            {
                return GetSecretsAsync(uri).Result;
            }
            else
            {
                throw new KeyVaultErrorException("Vault URI not set. Please use SetVault().");
            }

        }
        //Keys
        //Certificates
        //Management
        public void SetVault(string vaultURI)
        {
            vaulturi = vaultURI;
        }
        public string GetVaultURI(string vaultURI)
        {
            return vaultURI;
        }
        //Helper Functions
        private async Task<string> GetSecretAsync(string secretName)
        {
            SecretBundle sec = await vault.GetSecretAsync(vaulturi, secretName);
            if (sec.ContentType.Equals("base64String"))
            {
                return Encoding.UTF8.GetString(Convert.FromBase64String(sec.Value));
            }
            else
            {
                return sec.Value;
            }

        }
        private async Task<string> GetSecretFromURIAsync(string secretURI)
        {
            SecretBundle sec = await vault.GetSecretAsync(secretURI);
            if (sec.ContentType.Equals("base64String"))
            {
                return Encoding.UTF8.GetString(Convert.FromBase64String(sec.Value));
            }
            else
            {
                return sec.Value;
            }
        }
        private async Task<string> GetTokenFromCertAsync(string authority, string resource, string scope)
        {
            AuthenticationContext ctx = new AuthenticationContext(authority);
            ClientAssertionCertificate ast = new ClientAssertionCertificate(
              clientAppID, // application id
              authCertificate);
            AuthenticationResult authResult = await ctx.AcquireTokenAsync(resource, ast);
            if (authResult == null)
            {
                throw new InvalidOperationException("Failed to retrieve access token for Key Vault");
            }
            return authResult.AccessToken;

        }
        private async Task<string> GetTokenFromSecretAsync(string authority, string resource, string scope)
        {
            var authContext = new AuthenticationContext(authority);
            ClientCredential clientCred = new ClientCredential(clientAppID, clientAppSecret);
            AuthenticationResult result = await authContext.AcquireTokenAsync(resource, clientCred);
            if (result == null)
            {
                throw new InvalidOperationException("Failed to retrieve access token for Key Vault");
            }

            return result.AccessToken;
        }
        private async Task<List<string>> GetSecretsVersionAsync(string secretName, string uri)
        {
            List<string> ids = new List<string>();
            IPage<SecretItem> items = await vault.GetSecretVersionsAsync(uri, secretName);
            foreach (var item in items)
            {
                ids.Add(item.Id);
            }
            return ids;
        }
        private async Task<List<string>> GetSecretsAsync(string uri)
        {
            List<string> ids = new List<string>();
            IPage<SecretItem> items = await vault.GetSecretsAsync(uri);
            foreach (var item in items)
            {
                ids.Add(item.Id);
            }
            return ids;
        }

    }
}


